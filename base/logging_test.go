//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/natefinch/lumberjack.v2"
)

func TestRedactedLogFuncs(t *testing.T) {
	if GlobalTestLoggingSet.IsTrue() {
		t.Skip("Test does not work when a global test log level is set")
	}

	username := UD("alice")
	ctx := TestCtx(t)

	defer func() { RedactUserData = defaultRedactUserData }()

	RedactUserData = false
	AssertLogContains(t, "Username: alice", func() { InfofCtx(ctx, KeyAll, "Username: %s", username) })
	RedactUserData = true
	AssertLogContains(t, "Username: <ud>alice</ud>", func() { InfofCtx(ctx, KeyAll, "Username: %s", username) })

	RedactUserData = false
	AssertLogContains(t, "Username: alice", func() { WarnfCtx(ctx, "Username: %s", username) })
	RedactUserData = true
	AssertLogContains(t, "Username: <ud>alice</ud>", func() { WarnfCtx(ctx, "Username: %s", username) })
}

func Benchmark_LoggingPerformance(b *testing.B) {

	SetUpBenchmarkLogging(b, LevelInfo, KeyHTTP, KeyCRUD)

	ctx := TestCtx(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		DebugfCtx(ctx, KeyCRUD, "some crud'y message")
		InfofCtx(ctx, KeyCRUD, "some crud'y message")
		WarnfCtx(ctx, "some crud'y message")
		ErrorfCtx(ctx, "some crud'y message")
	}
}

func TestLogRotationInterval(t *testing.T) {
	LongRunningTest(t)

	// override min rotation interval for testing
	originalMinRotationInterval := minLogRotationInterval
	minLogRotationInterval = time.Millisecond * 10
	defer func() { minLogRotationInterval = originalMinRotationInterval }()

	rotationInterval := time.Millisecond * 100
	config := &FileLoggerConfig{
		Enabled:             BoolPtr(true),
		CollationBufferSize: IntPtr(0),
		Rotation: logRotationConfig{
			RotationInterval: NewConfigDuration(rotationInterval),
			compress:         BoolPtr(false),
		},
	}

	logPath := lumberjackTempDir(t)
	countBefore := numFilesInDir(t, logPath, false)
	t.Logf("countBefore: %d", countBefore)

	ctx := TestCtx(t)
	fl, err := NewFileLogger(ctx, config, LevelTrace, "test", logPath, 0, nil)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, fl.Close())
		// Wait for Lumberjack to finish its async log compression work
		// we have no way of waiting for this to finish, or even stopping the millRun() process inside Lumberjack.
		time.Sleep(time.Second)
	}()

	fl.logf("test 1")
	countAfter1 := numFilesInDir(t, logPath, false)
	t.Logf("countAfter1: %d", countAfter1)
	assert.Greater(t, countAfter1, countBefore)

	time.Sleep(rotationInterval * 5)
	countAfterSleep := numFilesInDir(t, logPath, false)
	t.Logf("countAfterSleep: %d", countAfterSleep)
	assert.Greater(t, countAfterSleep, countAfter1)

	fl.logf("test 2")
	countAfter2 := numFilesInDir(t, logPath, false)
	t.Logf("countAfter2: %d", countAfter2)
	assert.GreaterOrEqual(t, countAfter2, countAfterSleep)

}

// Benchmark the time it takes to write x bytes of data to a logger, and optionally rotate and compress it.
func BenchmarkLogRotation(b *testing.B) {

	tests := []struct {
		rotate   bool
		compress bool
		numBytes int
	}{
		{rotate: false, compress: false, numBytes: 0},
		{rotate: false, compress: false, numBytes: 1024 * 1000},   // 1MB
		{rotate: false, compress: false, numBytes: 1024 * 100000}, // 100MB
		{rotate: true, compress: false, numBytes: 0},
		{rotate: true, compress: false, numBytes: 1024 * 1000},   // 1MB
		{rotate: true, compress: false, numBytes: 1024 * 100000}, // 100MB
		{rotate: true, compress: true, numBytes: 0},
		{rotate: true, compress: true, numBytes: 1024 * 1000},   // 1MB
		{rotate: true, compress: true, numBytes: 1024 * 100000}, // 100MB
	}

	for _, test := range tests {
		b.Run(fmt.Sprintf("rotate:%t-compress:%t-bytes:%v", test.rotate, test.compress, test.numBytes), func(bm *testing.B) {
			data := FastRandBytes(bm, test.numBytes)

			logPath := b.TempDir()
			logger := lumberjack.Logger{Filename: filepath.Join(logPath, "output.log"), Compress: test.compress}

			bm.ResetTimer()
			for i := 0; i < bm.N; i++ {
				_, _ = logger.Write(data)
				if test.rotate {
					_ = logger.Rotate()
				}
			}
			bm.StopTimer()

			// Tidy up temp log files in a retry loop because
			// we can't remove temp dir while the async compression is still writing log files
			assert.NoError(bm, logger.Close())
			ctx := TestCtx(bm)
			err, _ := RetryLoop(ctx, "benchmark-logrotate-teardown",
				func() (shouldRetry bool, err error, value interface{}) {
					err = os.RemoveAll(logPath)
					return err != nil, err, nil
				},
				CreateDoublingSleeperFunc(3, 250),
			)
			assert.NoError(bm, err)
		})
	}

}

func TestLogColor(t *testing.T) {
	origColor := consoleLogger.Load().ColorEnabled
	defer func() { consoleLogger.Load().ColorEnabled = origColor }()

	consoleLogger.Load().ColorEnabled = true
	if colorEnabled() {
		assert.Equal(t, "\x1b[0;36mFormat\x1b[0m", color("Format", LevelDebug))
		assert.Equal(t, "\x1b[1;34mFormat\x1b[0m", color("Format", LevelInfo))
		assert.Equal(t, "\x1b[1;33mFormat\x1b[0m", color("Format", LevelWarn))
		assert.Equal(t, "\x1b[1;31mFormat\x1b[0m", color("Format", LevelError))
		assert.Equal(t, "\x1b[0;37mFormat\x1b[0m", color("Format", LevelTrace))
		assert.Equal(t, "\x1b[0mFormat\x1b[0m", color("Format", LevelNone))
	}

	consoleLogger.Load().ColorEnabled = false
	assert.Equal(t, "Format", color("Format", LevelDebug))
	assert.Equal(t, "Format", color("Format", LevelInfo))
	assert.Equal(t, "Format", color("Format", LevelWarn))
	assert.Equal(t, "Format", color("Format", LevelError))
	assert.Equal(t, "Format", color("Format", LevelTrace))
	assert.Equal(t, "Format", color("Format", LevelNone))
}

func BenchmarkLogColorEnabled(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skipf("color not supported in Windows")
	}

	b.Run("enabled", func(b *testing.B) {
		consoleLogger.Load().ColorEnabled = true
		require.NoError(b, os.Setenv("TERM", "xterm-256color"))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = colorEnabled()
		}
	})

	b.Run("disabled console color", func(b *testing.B) {
		consoleLogger.Load().ColorEnabled = false
		require.NoError(b, os.Setenv("TERM", "xterm-256color"))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = colorEnabled()
		}
	})

	b.Run("disabled term color", func(b *testing.B) {
		consoleLogger.Load().ColorEnabled = true
		require.NoError(b, os.Setenv("TERM", "dumb"))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = colorEnabled()
		}
	})
}

func TestGetCallersNameRecoverInfoImpossible(t *testing.T) {
	callerName := GetCallersName(3, true)
	assert.Equal(t, "???", callerName)
	callerName = GetCallersName(3, false)
	assert.Equal(t, "???", callerName)
}

func TestLastComponent(t *testing.T) {
	path := lastComponent("/var/log/sync_gateway/sglogfile.log")
	assert.Equal(t, "sglogfile.log", path)
	path = lastComponent("\\var\\log\\sync_gateway\\sglogfile.log")
	assert.Equal(t, "sglogfile.log", path)
	path = lastComponent("sglogfile.log")
	assert.Equal(t, "sglogfile.log", path)
	path = lastComponent("/sglogfile.log")
	assert.Equal(t, "sglogfile.log", path)
	path = lastComponent("\\sglogfile.log")
	assert.Equal(t, "sglogfile.log", path)
}

func TestLogSyncGatewayVersion(t *testing.T) {
	if GlobalTestLoggingSet.IsTrue() {
		t.Skip("Test does not work when a global test log level is set")
	}

	for i := LevelNone; i < levelCount; i++ {
		t.Run(i.String(), func(t *testing.T) {
			consoleLogger.Load().LogLevel.Set(i)
			out := CaptureConsolefLogOutput(func() { LogSyncGatewayVersion(TestCtx(t)) })
			assert.Contains(t, out, LongVersionString)
		})
	}
	consoleLogger.Load().LogLevel.Set(LevelInfo)
}

func CaptureConsolefLogOutput(f func()) string {
	buf := bytes.Buffer{}
	consoleFOutput = &buf
	f()
	consoleFOutput = os.Stderr
	return buf.String()
}

func BenchmarkGetCallersName(b *testing.B) {
	tests := []struct {
		depth       int
		includeLine bool
	}{
		{
			depth:       1,
			includeLine: false,
		},
		{
			depth:       2,
			includeLine: false,
		},
		{
			depth:       3,
			includeLine: false,
		},
		{
			// depth of 4 exceeds the call stack size for this benchnark
			// this should actually exit-early and be faster than the above
			depth:       4,
			includeLine: false,
		},
		{
			depth:       100,
			includeLine: false,
		},
		{
			depth:       1,
			includeLine: true,
		},
		{
			depth:       2,
			includeLine: true,
		},
		{
			depth:       3,
			includeLine: true,
		},
		{
			depth:       4,
			includeLine: true,
		},
		{
			depth:       100,
			includeLine: true,
		},
	}
	for _, tt := range tests {
		b.Run(fmt.Sprintf("%v-%v", tt.depth, tt.includeLine), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				GetCallersName(tt.depth, tt.includeLine)
			}
		})
	}
}

// lumberjackTempDir returns a temporary directory like t.Tempdir() but safe for lumberjack logs
func lumberjackTempDir(t *testing.T) string {
	if runtime.GOOS != "windows" {
		return t.TempDir()
	}
	// On Windows, cleanup of t.TempDir() fails due to open log file handle from Lumberjack. Cannot be fixed from SG.
	// https://github.com/natefinch/lumberjack/issues/185
	logPath, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := os.RemoveAll(logPath); err != nil {
			// log instead of error because it's likely this is going to fail on Windows for this test.
			t.Logf("couldn't remove temp dir: %v, files: %s", err, getDirFiles(t, logPath))
		}
	})
	return logPath
}

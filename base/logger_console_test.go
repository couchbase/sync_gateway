/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var consoleShouldLogTests = []struct {
	loggerLevel LogLevel
	logToLevel  LogLevel
	loggerKeys  []string
	logToKey    LogKey
	expected    bool
}{
	{
		// Log with matching log level and key
		loggerLevel: LevelInfo,
		logToLevel:  LevelInfo,
		loggerKeys:  []string{"HTTP"},
		logToKey:    KeyHTTP,
		expected:    true,
	},
	{
		// Log with higher log level and matching key
		loggerLevel: LevelInfo,
		logToLevel:  LevelWarn,
		loggerKeys:  []string{"HTTP"},
		logToKey:    KeyHTTP,
		expected:    true,
	},
	{
		// Log with lower log level and matching key
		loggerLevel: LevelWarn,
		logToLevel:  LevelInfo,
		loggerKeys:  []string{"HTTP"},
		logToKey:    KeyHTTP,
		expected:    false,
	},
	{
		// Logger disabled (LevelNone)
		loggerLevel: LevelNone,
		logToLevel:  LevelError,
		loggerKeys:  []string{"HTTP"},
		logToKey:    KeyHTTP,
		expected:    false,
	},
	{
		// Logger disabled (No keys)
		loggerLevel: LevelInfo,
		logToLevel:  LevelInfo,
		loggerKeys:  []string{},
		logToKey:    KeyDCP,
		expected:    false,
	},
	{
		// Log with matching log level and unmatched key
		loggerLevel: LevelInfo,
		logToLevel:  LevelInfo,
		loggerKeys:  []string{"HTTP"},
		logToKey:    KeyDCP,
		expected:    false,
	},
	{
		// Log with matching log level and wildcard key
		loggerLevel: LevelInfo,
		logToLevel:  LevelInfo,
		loggerKeys:  []string{"*"},
		logToKey:    KeyDCP,
		expected:    true,
	},
	{
		// Log with lower log level and wildcard key
		loggerLevel: LevelWarn,
		logToLevel:  LevelInfo,
		loggerKeys:  []string{"*"},
		logToKey:    KeyDCP,
		expected:    false,
	},
}

func TestConsoleShouldLog(t *testing.T) {
	for _, test := range consoleShouldLogTests {
		name := fmt.Sprintf("logger{%s,%s}.shouldLog(%s,%s)",
			test.loggerLevel.StringShort(), test.loggerKeys,
			test.logToLevel.StringShort(), test.logToKey)

		level := test.loggerLevel
		l := mustInitConsoleLogger(TestCtx(t), &ConsoleLoggerConfig{
			LogLevel: &level,
			LogKeys:  test.loggerKeys,
			FileLoggerConfig: FileLoggerConfig{
				Enabled: BoolPtr(true),
				Output:  io.Discard,
			}})

		t.Run(name, func(ts *testing.T) {
			got := l.shouldLog(TestCtx(ts), test.logToLevel, test.logToKey)
			assert.Equal(ts, test.expected, got)
		})
	}
}

func BenchmarkConsoleShouldLog(b *testing.B) {
	for _, test := range consoleShouldLogTests {
		name := fmt.Sprintf("logger{%s,%s}.shouldLog(%s,%s)",
			test.loggerLevel.StringShort(), test.loggerKeys,
			test.logToLevel.StringShort(), test.logToKey)

		level := test.loggerLevel
		l := mustInitConsoleLogger(TestCtx(b), &ConsoleLoggerConfig{
			LogLevel: &level,
			LogKeys:  test.loggerKeys,
			FileLoggerConfig: FileLoggerConfig{
				Enabled: BoolPtr(true),
				Output:  io.Discard,
			}})

		b.Run(name, func(bb *testing.B) {
			for i := 0; i < bb.N; i++ {
				l.shouldLog(TestCtx(bb), test.logToLevel, test.logToKey)
			}
		})
	}
}

// TestConsoleShouldLogWithDatabase ensures that if set, database log config takes precedence over console config.
func TestConsoleShouldLogWithDatabase(t *testing.T) {
	for _, test := range consoleShouldLogTests {
		for _, dbConfig := range []struct {
			consoleConfig *DbConsoleLogConfig
			expected      bool
		}{
			{
				consoleConfig: nil,
				expected:      test.expected, // fully inherit from console logger when dbConfig nil
			},
			{
				consoleConfig: &DbConsoleLogConfig{
					LogLevel: logLevelPtr(LevelNone),
					LogKeys:  logKeyMask(KeyAll),
				},
				expected: false, // log nothing from db
			},
			{
				consoleConfig: &DbConsoleLogConfig{
					LogLevel: logLevelPtr(LevelTrace),
					LogKeys:  logKeyMask(KeyAll),
				},
				expected: true, // log everything from db
			},
			{
				consoleConfig: &DbConsoleLogConfig{
					LogLevel: logLevelPtr(LevelInfo),
					LogKeys:  logKeyMask(KeyDCP),
				},
				// log DCP only from db (overrides console key)
				expected: test.logToKey == KeyDCP && test.logToLevel <= LevelInfo,
			},
			{
				consoleConfig: &DbConsoleLogConfig{
					LogLevel: logLevelPtr(LevelInfo),
					LogKeys:  logKeyMask(KeyHTTP),
				},
				// Still expect the HTTP warnings when Info is set on db
				expected: test.logToKey == KeyHTTP && test.logToLevel <= LevelInfo,
			},
		} {
			dbConfigLevel := "<nil>"
			if dbConfig.consoleConfig != nil && dbConfig.consoleConfig.LogLevel != nil {
				dbConfigLevel = dbConfig.consoleConfig.LogLevel.StringShort()
			}
			dbConfigKeys := "<nil>"
			if dbConfig.consoleConfig != nil && dbConfig.consoleConfig.LogKeys != nil {
				dbConfigKeys = dbConfig.consoleConfig.LogKeys.String()
			}

			name := fmt.Sprintf("logger{%s,%s}.shouldLog(dbConfig(%s,%s), %s,%s)",
				test.loggerLevel.StringShort(), test.loggerKeys,
				dbConfigLevel, dbConfigKeys,
				test.logToLevel.StringShort(), test.logToKey)

			level := test.loggerLevel
			l := mustInitConsoleLogger(TestCtx(t), &ConsoleLoggerConfig{
				LogLevel: &level,
				LogKeys:  test.loggerKeys,
				FileLoggerConfig: FileLoggerConfig{
					Enabled: BoolPtr(true),
					Output:  io.Discard,
				}})

			t.Run(name, func(ts *testing.T) {
				config := &DbLogConfig{Console: dbConfig.consoleConfig}
				ctx := DatabaseLogCtx(TestCtx(ts), "db", config)
				got := l.shouldLog(ctx, test.logToLevel, test.logToKey)
				assert.Equal(ts, dbConfig.expected, got)
			})
		}
	}
}

func TestConsoleLogDefaults(t *testing.T) {
	tests := []struct {
		name     string
		config   ConsoleLoggerConfig
		expected ConsoleLogger
	}{
		{
			name:   "empty",
			config: ConsoleLoggerConfig{},
			expected: ConsoleLogger{
				FileLogger: FileLogger{Enabled: AtomicBool{0}},
				LogLevel:   logLevelPtr(LevelNone),
				LogKeyMask: logKeyMask(KeyNone),
				config:     ConsoleLoggerConfig{LogKeys: []string{}},
				isStderr:   false,
			},
		},
		{
			name:   "key",
			config: ConsoleLoggerConfig{LogKeys: []string{"CRUD"}},
			expected: ConsoleLogger{
				FileLogger: FileLogger{Enabled: AtomicBool{1}},
				LogLevel:   logLevelPtr(LevelInfo),
				LogKeyMask: logKeyMask(KeyCRUD),
				config:     ConsoleLoggerConfig{LogKeys: []string{"CRUD"}},
				isStderr:   true,
			},
		},
		{
			name:   "level",
			config: ConsoleLoggerConfig{LogLevel: logLevelPtr(LevelWarn)},
			expected: ConsoleLogger{
				FileLogger: FileLogger{Enabled: AtomicBool{1}},
				LogLevel:   logLevelPtr(LevelWarn),
				LogKeyMask: logKeyMask(KeyNone),
				config:     ConsoleLoggerConfig{LogKeys: nil},
				isStderr:   true,
			},
		},
		{
			name:   "level and key",
			config: ConsoleLoggerConfig{LogLevel: logLevelPtr(LevelWarn), LogKeys: []string{"CRUD"}},
			expected: ConsoleLogger{
				FileLogger: FileLogger{Enabled: AtomicBool{1}},
				LogLevel:   logLevelPtr(LevelWarn),
				LogKeyMask: logKeyMask(KeyCRUD),
				config:     ConsoleLoggerConfig{LogKeys: []string{"CRUD"}},
				isStderr:   true,
			},
		},
		{
			name:   "http default",
			config: ConsoleLoggerConfig{LogKeys: []string{"HTTP"}},
			expected: ConsoleLogger{
				FileLogger: FileLogger{Enabled: AtomicBool{1}},
				LogLevel:   logLevelPtr(LevelInfo),
				LogKeyMask: logKeyMask(KeyHTTP),
				config:     ConsoleLoggerConfig{LogKeys: []string{"HTTP"}},
				isStderr:   true,
			},
		},
	}

	for _, test := range tests {
		config := test.config
		t.Run(test.name, func(tt *testing.T) {
			logger, err := NewConsoleLogger(TestCtx(tt), false, &config)
			assert.NoError(tt, err)
			assert.Equal(tt, test.expected.Enabled, logger.Enabled)
			assert.Equal(tt, *test.expected.LogLevel, *logger.LogLevel)
			assert.Equal(tt, *test.expected.LogKeyMask, *logger.LogKeyMask)
			assert.Equal(tt, test.expected.isStderr, logger.isStderr)
			assert.Equal(tt, test.expected.config.LogKeys, logger.config.LogKeys)
		})
	}
}

func TestConsoleIrregularLogPaths(t *testing.T) {
	// override min rotation interval for testing
	originalMinRotationInterval := minLogRotationInterval
	minLogRotationInterval = time.Millisecond * 10
	defer func() { minLogRotationInterval = originalMinRotationInterval }()

	testCases := []struct {
		name    string
		logPath string
	}{
		{
			name:    ".log extension",
			logPath: "foo.log",
			// take foo-2021-01-01T00-00-00.000.log
		},
		{
			name:    "no extension",
			logPath: "foo",
			// foo-2021-01-01T00-00-00.000
		},
		{
			name:    "multiple dots",
			logPath: "two.ext.log",
			// two.ext-2021-01-01T00-00-00.000.log
		},
		{
			name:    "start with .",
			logPath: ".hidden.log",
			// .hidden-2021-01-01T00-00-00.000.log
		},
		{
			name:    "start with ., no ext",
			logPath: ".hidden",
			// -2021-01-01T00-00-00.000.hidden
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			tempdir := lumberjackTempDir(t)
			config := &ConsoleLoggerConfig{
				LogLevel:   logLevelPtr(LevelDebug),
				LogKeys:    []string{"HTTP"},
				FileOutput: filepath.Join(tempdir, test.logPath),
				FileLoggerConfig: FileLoggerConfig{
					Enabled:             BoolPtr(true),
					CollationBufferSize: IntPtr(0),
					Rotation: logRotationConfig{
						RotationInterval: NewConfigDuration(10 * time.Millisecond),
					},
				}}

			ctx := TestCtx(t)
			logger, err := NewConsoleLogger(ctx, false, config)
			require.NoError(t, err)

			// ensure logging is done before closing the logger
			wg := sync.WaitGroup{}
			wg.Add(1)
			doneChan := make(chan struct{})
			defer func() {
				close(doneChan)
				wg.Wait()
				assert.NoError(t, logger.Close())
			}()
			go func() {
				for {
					select {
					case <-doneChan:
						wg.Done()
						return
					default:
						logger.logf("some text")
					}
				}
			}()
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				filenames := getDirFiles(t, tempdir)
				assert.Contains(c, filenames, test.logPath)
				assert.Greater(c, len(filenames), 2)
			}, time.Second, 10*time.Millisecond)

			// add a few non-matching files to the directory for negative testing
			nonMatchingFileNames := []string{
				"console.log",
				"consoellog",
				"console.log.txt",
				".console",
				"consolelog-2021-01-01T00-00-00.000",
				"console-2021-01-01T00-00-00.000.log",
			}
			for _, name := range nonMatchingFileNames {
				require.NoError(t, makeTestFile(1, name, tempdir))
			}

			_, pattern := getDeletionDirAndRegexp(filepath.Join(tempdir, test.logPath))
			for _, filename := range getDirFiles(t, tempdir) {
				if slices.Contains(nonMatchingFileNames, filename) {
					require.NotRegexp(t, pattern, filename)
				} else {
					require.Regexp(t, pattern, filename)
				}
			}
		})
	}
}

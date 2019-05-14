package base

import (
	"fmt"
	"io/ioutil"
	"testing"

	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
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

		l := newConsoleLoggerOrPanic(&ConsoleLoggerConfig{
			LogLevel: &test.loggerLevel,
			LogKeys:  test.loggerKeys,
			FileLoggerConfig: FileLoggerConfig{
				Enabled: BoolPtr(true),
				Output:  ioutil.Discard,
			}})

		t.Run(name, func(ts *testing.T) {
			got := l.shouldLog(test.logToLevel, test.logToKey)
			goassert.Equals(ts, got, test.expected)
		})
	}
}

func BenchmarkConsoleShouldLog(b *testing.B) {
	for _, test := range consoleShouldLogTests {
		name := fmt.Sprintf("logger{%s,%s}.shouldLog(%s,%s)",
			test.loggerLevel.StringShort(), test.loggerKeys,
			test.logToLevel.StringShort(), test.logToKey)

		l := newConsoleLoggerOrPanic(&ConsoleLoggerConfig{
			LogLevel: &test.loggerLevel,
			LogKeys:  test.loggerKeys,
			FileLoggerConfig: FileLoggerConfig{
				Enabled: BoolPtr(true),
				Output:  ioutil.Discard,
			}})

		b.Run(name, func(bb *testing.B) {
			for i := 0; i < bb.N; i++ {
				l.shouldLog(test.logToLevel, test.logToKey)
			}
		})
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
				FileLogger: FileLogger{Enabled: false},
				LogLevel:   logLevelPtr(LevelNone),
				LogKey:     logKeyPtr(KeyHTTP),
				isStderr:   false,
			},
		},
		{
			name:   "key",
			config: ConsoleLoggerConfig{LogKeys: []string{"CRUD"}},
			expected: ConsoleLogger{
				FileLogger: FileLogger{Enabled: true},
				LogLevel:   logLevelPtr(LevelInfo),
				LogKey:     logKeyPtr(KeyHTTP | KeyCRUD),
				isStderr:   true,
			},
		},
		{
			name:   "level",
			config: ConsoleLoggerConfig{LogLevel: logLevelPtr(LevelWarn)},
			expected: ConsoleLogger{
				FileLogger: FileLogger{Enabled: true},
				LogLevel:   logLevelPtr(LevelWarn),
				LogKey:     logKeyPtr(KeyHTTP),
				isStderr:   true,
			},
		},
		{
			name:   "level and key",
			config: ConsoleLoggerConfig{LogLevel: logLevelPtr(LevelWarn), LogKeys: []string{"CRUD"}},
			expected: ConsoleLogger{
				FileLogger: FileLogger{Enabled: true},
				LogLevel:   logLevelPtr(LevelWarn),
				LogKey:     logKeyPtr(KeyHTTP | KeyCRUD),
				isStderr:   true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			logger, _, err := NewConsoleLogger(&test.config)
			assert.NoError(tt, err)
			assert.Equal(tt, test.expected.Enabled, logger.Enabled)
			assert.Equal(tt, *test.expected.LogLevel, *logger.LogLevel)
			assert.Equal(tt, *test.expected.LogKey, *logger.LogKey)
			assert.Equal(tt, test.expected.isStderr, logger.isStderr)
		})
	}
}

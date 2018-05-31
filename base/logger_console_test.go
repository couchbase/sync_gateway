package base

import (
	"fmt"
	"io/ioutil"
	"testing"

	assert "github.com/couchbaselabs/go.assert"
)

func TestConsoleShouldLog(t *testing.T) {
	tests := []struct {
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

	for _, test := range tests {
		name := fmt.Sprintf("logger{%s,%s}.shouldLog(%s,%s)",
			test.loggerLevel.StringShort(), test.loggerKeys,
			test.logToLevel.StringShort(), test.logToKey)

		l := newConsoleLoggerOrPanic(&ConsoleLoggerConfig{
			LogLevel: &test.loggerLevel,
			LogKeys:  test.loggerKeys,
			Output:   ioutil.Discard,
		})

		t.Run(name, func(ts *testing.T) {
			got := l.shouldLog(test.logToLevel, test.logToKey)
			assert.Equals(ts, got, test.expected)
		})
	}
}

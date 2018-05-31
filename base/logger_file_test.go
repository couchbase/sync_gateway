package base

import (
	"fmt"
	"io/ioutil"
	"log"
	"testing"

	assert "github.com/couchbaselabs/go.assert"
)

func TestFileShouldLog(t *testing.T) {
	tests := []struct {
		enabled     bool
		loggerLevel LogLevel
		logToLevel  LogLevel
		loggerKeys  []string
		logToKey    LogKey
		expected    bool
	}{
		{
			// Log with matching log level
			enabled:     true,
			loggerLevel: LevelInfo,
			logToLevel:  LevelInfo,
			expected:    true,
		},
		{
			// Log with higher log level
			enabled:     true,
			loggerLevel: LevelInfo,
			logToLevel:  LevelWarn,
			expected:    true,
		},
		{
			// Log with lower log level
			enabled:     true,
			loggerLevel: LevelWarn,
			logToLevel:  LevelInfo,
			expected:    false,
		},
		{
			// Logger disabled (enabled = false)
			enabled:     false,
			loggerLevel: LevelNone,
			logToLevel:  LevelError,
			expected:    false,
		},
		{
			// Logger disabled (LevelNone)
			enabled:     true,
			loggerLevel: LevelNone,
			logToLevel:  LevelInfo,
			expected:    false,
		},
	}

	for _, test := range tests {
		name := fmt.Sprintf("logger{%s,%s}.shouldLog(%s,%s)",
			test.loggerLevel.StringShort(), test.loggerKeys,
			test.logToLevel.StringShort(), test.logToKey)

		l := FileLogger{
			Enabled: test.enabled,
			level:   test.loggerLevel,
			output:  ioutil.Discard,
			logger:  log.New(ioutil.Discard, "", 0),
		}

		t.Run(name, func(ts *testing.T) {
			got := l.shouldLog(test.logToLevel)
			assert.Equals(ts, got, test.expected)
		})
	}
}

package base

import (
	"fmt"
	"math"
	"testing"
	"time"

	assert "github.com/couchbaselabs/go.assert"
)

// Tests the cascading behaviour of log levels.
func TestLogLevel(t *testing.T) {
	var logLevelPtr *LogLevel
	assert.False(t, logLevelPtr.Enabled(LevelError))
	assert.False(t, logLevelPtr.Enabled(LevelWarn))
	assert.False(t, logLevelPtr.Enabled(LevelInfo))
	assert.False(t, logLevelPtr.Enabled(LevelDebug))
	assert.False(t, logLevelPtr.Enabled(LevelTrace))

	logLevel := LevelNone
	assert.False(t, logLevel.Enabled(LevelError))
	assert.False(t, logLevel.Enabled(LevelWarn))
	assert.False(t, logLevel.Enabled(LevelInfo))
	assert.False(t, logLevel.Enabled(LevelDebug))
	assert.False(t, logLevel.Enabled(LevelTrace))

	logLevel.Set(LevelError)
	assert.True(t, logLevel.Enabled(LevelError))
	assert.False(t, logLevel.Enabled(LevelWarn))
	assert.False(t, logLevel.Enabled(LevelInfo))
	assert.False(t, logLevel.Enabled(LevelDebug))
	assert.False(t, logLevel.Enabled(LevelTrace))

	logLevel.Set(LevelWarn)
	assert.True(t, logLevel.Enabled(LevelError))
	assert.True(t, logLevel.Enabled(LevelWarn))
	assert.False(t, logLevel.Enabled(LevelInfo))
	assert.False(t, logLevel.Enabled(LevelDebug))
	assert.False(t, logLevel.Enabled(LevelTrace))

	logLevel.Set(LevelInfo)
	assert.True(t, logLevel.Enabled(LevelError))
	assert.True(t, logLevel.Enabled(LevelWarn))
	assert.True(t, logLevel.Enabled(LevelInfo))
	assert.False(t, logLevel.Enabled(LevelDebug))
	assert.False(t, logLevel.Enabled(LevelTrace))

	logLevel.Set(LevelDebug)
	assert.True(t, logLevel.Enabled(LevelError))
	assert.True(t, logLevel.Enabled(LevelWarn))
	assert.True(t, logLevel.Enabled(LevelInfo))
	assert.True(t, logLevel.Enabled(LevelDebug))
	assert.False(t, logLevel.Enabled(LevelTrace))

	logLevel.Set(LevelTrace)
	assert.True(t, logLevel.Enabled(LevelError))
	assert.True(t, logLevel.Enabled(LevelWarn))
	assert.True(t, logLevel.Enabled(LevelInfo))
	assert.True(t, logLevel.Enabled(LevelDebug))
	assert.True(t, logLevel.Enabled(LevelTrace))
}

func TestLogLevelNames(t *testing.T) {
	// Ensure number of level constants, and names match.
	assert.Equals(t, len(logLevelNames), int(levelCount))
	assert.Equals(t, len(logLevelNamesPrint), int(levelCount))

	assert.Equals(t, LevelNone.String(), "none")
	assert.Equals(t, LevelError.String(), "error")
	assert.Equals(t, LevelInfo.String(), "info")
	assert.Equals(t, LevelWarn.String(), "warn")
	assert.Equals(t, LevelDebug.String(), "debug")
	assert.Equals(t, LevelTrace.String(), "trace")

	// Test out of bounds log level
	assert.Equals(t, LogLevel(math.MaxUint32).String(), "LogLevel(4294967295)")
}

func TestLogLevelText(t *testing.T) {
	for i := LogLevel(0); i < levelCount; i++ {
		t.Run(fmt.Sprintf("LogLevel: %v", i), func(ts *testing.T) {
			text, err := i.MarshalText()
			assert.Equals(ts, err, nil)
			assert.Equals(ts, string(text), i.String())

			var logLevel LogLevel
			err = logLevel.UnmarshalText(text)
			assert.Equals(ts, err, nil)
			assert.Equals(ts, logLevel, i)
		})
	}

	// nil pointer recievers
	var logLevelPtr *LogLevel
	_, err := logLevelPtr.MarshalText()
	assert.StringContains(t, err.Error(), "unrecognized log level")
	err = logLevelPtr.UnmarshalText([]byte("none"))
	assert.Equals(t, err.Error(), "nil log level")

	// Invalid values
	var logLevel = LogLevel(math.MaxUint32)
	text, err := logLevel.MarshalText()
	assert.NotEquals(t, err, nil)
	assert.Equals(t, string(text), "")

	logLevel = LevelNone
	err = logLevel.UnmarshalText([]byte(""))
	assert.NotEquals(t, err, nil)
	assert.Equals(t, logLevel, LevelNone)
}

// This test has no assertions, but will flag any data races when run under `-race`.
func TestLogLevelConcurrency(t *testing.T) {
	logLevel := LevelWarn
	stop := make(chan struct{})

	go func() {
		for {
			select {
			default:
				logLevel.Set(LevelError)
			case <-stop:
				return
			}
		}
	}()

	go func() {
		for {
			select {
			default:
				logLevel.Set(LevelDebug)
			case <-stop:
				return
			}
		}
	}()

	go func() {
		for {
			select {
			default:
				_ = logLevel.Enabled(LevelWarn)
			case <-stop:
				return
			}
		}
	}()

	time.Sleep(time.Millisecond * 100)
	stop <- struct{}{}
}

func BenchmarkLogLevelName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = LevelWarn.String()
	}
}

func BenchmarkLogLevelEnabled(b *testing.B) {
	var tests = []struct {
		Name         string
		TestLogLevel LogLevel
		SetLogLevel  LogLevel
	}{
		{"Hit", LevelError, LevelInfo},
		{"Miss", LevelDebug, LevelInfo},
	}

	for _, t := range tests {
		b.Run(t.Name, func(bn *testing.B) {
			for i := 0; i < bn.N; i++ {
				t.SetLogLevel.Enabled(t.TestLogLevel)
			}
		})
	}
}

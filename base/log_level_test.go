package base

import (
	"fmt"
	"math"
	"testing"
	"time"

	goassert "github.com/couchbaselabs/go.assert"
)

// Tests the cascading behaviour of log levels.
func TestLogLevel(t *testing.T) {
	var logLevelPtr *LogLevel
	goassert.False(t, logLevelPtr.Enabled(LevelError))
	goassert.False(t, logLevelPtr.Enabled(LevelWarn))
	goassert.False(t, logLevelPtr.Enabled(LevelInfo))
	goassert.False(t, logLevelPtr.Enabled(LevelDebug))
	goassert.False(t, logLevelPtr.Enabled(LevelTrace))

	logLevel := LevelNone
	goassert.False(t, logLevel.Enabled(LevelError))
	goassert.False(t, logLevel.Enabled(LevelWarn))
	goassert.False(t, logLevel.Enabled(LevelInfo))
	goassert.False(t, logLevel.Enabled(LevelDebug))
	goassert.False(t, logLevel.Enabled(LevelTrace))

	logLevel.Set(LevelError)
	goassert.True(t, logLevel.Enabled(LevelError))
	goassert.False(t, logLevel.Enabled(LevelWarn))
	goassert.False(t, logLevel.Enabled(LevelInfo))
	goassert.False(t, logLevel.Enabled(LevelDebug))
	goassert.False(t, logLevel.Enabled(LevelTrace))

	logLevel.Set(LevelWarn)
	goassert.True(t, logLevel.Enabled(LevelError))
	goassert.True(t, logLevel.Enabled(LevelWarn))
	goassert.False(t, logLevel.Enabled(LevelInfo))
	goassert.False(t, logLevel.Enabled(LevelDebug))
	goassert.False(t, logLevel.Enabled(LevelTrace))

	logLevel.Set(LevelInfo)
	goassert.True(t, logLevel.Enabled(LevelError))
	goassert.True(t, logLevel.Enabled(LevelWarn))
	goassert.True(t, logLevel.Enabled(LevelInfo))
	goassert.False(t, logLevel.Enabled(LevelDebug))
	goassert.False(t, logLevel.Enabled(LevelTrace))

	logLevel.Set(LevelDebug)
	goassert.True(t, logLevel.Enabled(LevelError))
	goassert.True(t, logLevel.Enabled(LevelWarn))
	goassert.True(t, logLevel.Enabled(LevelInfo))
	goassert.True(t, logLevel.Enabled(LevelDebug))
	goassert.False(t, logLevel.Enabled(LevelTrace))

	logLevel.Set(LevelTrace)
	goassert.True(t, logLevel.Enabled(LevelError))
	goassert.True(t, logLevel.Enabled(LevelWarn))
	goassert.True(t, logLevel.Enabled(LevelInfo))
	goassert.True(t, logLevel.Enabled(LevelDebug))
	goassert.True(t, logLevel.Enabled(LevelTrace))
}

func TestLogLevelNames(t *testing.T) {
	// Ensure number of level constants, and names match.
	goassert.Equals(t, len(logLevelNames), int(levelCount))
	goassert.Equals(t, len(logLevelNamesPrint), int(levelCount))

	goassert.Equals(t, LevelNone.String(), "none")
	goassert.Equals(t, LevelError.String(), "error")
	goassert.Equals(t, LevelInfo.String(), "info")
	goassert.Equals(t, LevelWarn.String(), "warn")
	goassert.Equals(t, LevelDebug.String(), "debug")
	goassert.Equals(t, LevelTrace.String(), "trace")

	// Test out of bounds log level
	goassert.Equals(t, LogLevel(math.MaxUint32).String(), "LogLevel(4294967295)")
}

func TestLogLevelText(t *testing.T) {
	for i := LogLevel(0); i < levelCount; i++ {
		t.Run(fmt.Sprintf("LogLevel: %v", i), func(ts *testing.T) {
			text, err := i.MarshalText()
			goassert.Equals(ts, err, nil)
			goassert.Equals(ts, string(text), i.String())

			var logLevel LogLevel
			err = logLevel.UnmarshalText(text)
			goassert.Equals(ts, err, nil)
			goassert.Equals(ts, logLevel, i)
		})
	}

	// nil pointer recievers
	var logLevelPtr *LogLevel
	_, err := logLevelPtr.MarshalText()
	goassert.StringContains(t, err.Error(), "unrecognized log level")
	err = logLevelPtr.UnmarshalText([]byte("none"))
	goassert.Equals(t, err.Error(), "nil log level")

	// Invalid values
	var logLevel = LogLevel(math.MaxUint32)
	text, err := logLevel.MarshalText()
	goassert.NotEquals(t, err, nil)
	goassert.Equals(t, string(text), "")

	logLevel = LevelNone
	err = logLevel.UnmarshalText([]byte(""))
	goassert.NotEquals(t, err, nil)
	goassert.Equals(t, logLevel, LevelNone)
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

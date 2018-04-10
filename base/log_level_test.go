package base

import (
	"fmt"
	"math"
	"testing"
	"time"

	assert "github.com/couchbaselabs/go.assert"
)

func TestLogLevel(t *testing.T) {
	var logLevelPtr *LogLevel
	assert.False(t, logLevelPtr.Enabled(LevelDebug))
	assert.False(t, logLevelPtr.Enabled(LevelInfo))
	assert.False(t, logLevelPtr.Enabled(LevelWarn))
	assert.False(t, logLevelPtr.Enabled(LevelError))

	logLevel := LevelNone
	assert.False(t, logLevel.Enabled(LevelDebug))
	assert.False(t, logLevel.Enabled(LevelInfo))
	assert.False(t, logLevel.Enabled(LevelWarn))
	assert.False(t, logLevel.Enabled(LevelError))

	logLevel.Set(LevelInfo)
	assert.False(t, logLevel.Enabled(LevelDebug))
	assert.True(t, logLevel.Enabled(LevelInfo))
	assert.True(t, logLevel.Enabled(LevelWarn))
	assert.True(t, logLevel.Enabled(LevelError))

	logLevel.Set(LevelWarn)
	assert.False(t, logLevel.Enabled(LevelDebug))
	assert.False(t, logLevel.Enabled(LevelInfo))
	assert.True(t, logLevel.Enabled(LevelWarn))
	assert.True(t, logLevel.Enabled(LevelError))
}

func TestLogLevelNames(t *testing.T) {
	// Ensure number of level constants, and names match.
	assert.Equals(t, len(logLevelNames), int(levelCount))

	assert.Equals(t, LogLevelName(LevelNone), "none")
	assert.Equals(t, LogLevelName(LevelError), "error")
	assert.Equals(t, LogLevelName(LevelInfo), "info")
	assert.Equals(t, LogLevelName(LevelWarn), "warn")
	assert.Equals(t, LogLevelName(LevelDebug), "debug")

	assert.Equals(t, LogLevelName(math.MaxUint32), "")
}

func TestLogLevelText(t *testing.T) {
	for i := LogLevel(0); i < levelCount; i++ {
		t.Run(fmt.Sprintf("LogLevel: %v", i), func(ts *testing.T) {
			text, err := i.MarshalText()
			assert.Equals(ts, err, nil)
			assert.Equals(ts, string(text), LogLevelName(i))

			var logLevel LogLevel
			err = logLevel.UnmarshalText(text)
			assert.Equals(ts, err, nil)
			assert.Equals(ts, logLevel, i)
		})
	}

	// nil pointer recievers
	var logLevelPtr *LogLevel
	_, err := logLevelPtr.MarshalText()
	assert.Equals(t, err.Error(), "invalid log level")
	err = logLevelPtr.UnmarshalText([]byte("none"))
	assert.Equals(t, err.Error(), "invalid log level")

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
		_ = LogLevelName(LevelWarn)
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

package base

import (
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
	assert.Equals(t, LogLevelName(LevelNone), "none")
	assert.Equals(t, LogLevelName(LevelError), "error")
	assert.Equals(t, LogLevelName(LevelInfo), "info")
	assert.Equals(t, LogLevelName(LevelWarn), "warn")
	assert.Equals(t, LogLevelName(LevelDebug), "debug")
}

func TestLogLevelText(t *testing.T) {
	var logLevelPtr *LogLevel
	text, err := logLevelPtr.MarshalText()
	assert.Equals(t, err.Error(), "invalid log level")
	err = logLevelPtr.UnmarshalText(text)
	assert.NotEquals(t, err, nil)

	var logLevel LogLevel
	text, err = logLevel.MarshalText()
	assert.Equals(t, err, nil)
	assert.Equals(t, string(text), "none")
	err = logLevel.UnmarshalText(text)
	assert.Equals(t, err, nil)
	assert.Equals(t, logLevel, LevelNone)

	logLevel.Set(LevelDebug)
	text, err = logLevel.MarshalText()
	assert.Equals(t, err, nil)
	assert.Equals(t, string(text), "debug")
	err = logLevel.UnmarshalText(text)
	assert.Equals(t, err, nil)
	assert.Equals(t, logLevel, LevelDebug)

	logLevel.Set(LevelInfo)
	text, err = logLevel.MarshalText()
	assert.Equals(t, err, nil)
	assert.Equals(t, string(text), "info")
	err = logLevel.UnmarshalText(text)
	assert.Equals(t, err, nil)
	assert.Equals(t, logLevel, LevelInfo)

	logLevel.Set(LevelWarn)
	text, err = logLevel.MarshalText()
	assert.Equals(t, err, nil)
	assert.Equals(t, string(text), "warn")
	err = logLevel.UnmarshalText(text)
	assert.Equals(t, err, nil)
	assert.Equals(t, logLevel, LevelWarn)

	logLevel.Set(LevelError)
	text, err = logLevel.MarshalText()
	assert.Equals(t, err, nil)
	assert.Equals(t, string(text), "error")
	err = logLevel.UnmarshalText(text)
	assert.Equals(t, err, nil)
	assert.Equals(t, logLevel, LevelError)
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
	logLevel := LevelInfo
	benchmarkLogLevelEnabled(b, "Hit", LevelError, logLevel)
	benchmarkLogLevelEnabled(b, "Miss", LevelDebug, logLevel)
}

func benchmarkLogLevelEnabled(b *testing.B, name string, l LogLevel, logLevel LogLevel) {
	b.Run(name, func(bn *testing.B) {
		for i := 0; i < bn.N; i++ {
			logLevel.Enabled(l)
		}
	})
}

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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	assert.Equal(t, int(levelCount), len(logLevelNames))
	assert.Equal(t, int(levelCount), len(logLevelNamesPrint))

	assert.Equal(t, "none", LevelNone.String())
	assert.Equal(t, "error", LevelError.String())
	assert.Equal(t, "info", LevelInfo.String())
	assert.Equal(t, "warn", LevelWarn.String())
	assert.Equal(t, "debug", LevelDebug.String())
	assert.Equal(t, "trace", LevelTrace.String())

	// Test out of bounds log level
	assert.Equal(t, "LogLevel(4294967295)", LogLevel(math.MaxUint32).String())
}

func TestLogLevelText(t *testing.T) {
	for i := LogLevel(0); i < levelCount; i++ {
		t.Run(fmt.Sprintf("LogLevel: %v", i), func(ts *testing.T) {
			text, err := i.MarshalText()
			assert.NoError(ts, err)
			assert.Equal(ts, i.String(), string(text))

			var logLevel LogLevel
			err = logLevel.UnmarshalText(text)
			assert.NoError(ts, err)
			assert.Equal(ts, i, logLevel)
		})
	}

	// nil pointer recievers
	var logLevelPtr *LogLevel
	_, err := logLevelPtr.MarshalText()
	assert.Contains(t, err.Error(), "unrecognized log level")
	err = logLevelPtr.UnmarshalText([]byte("none"))
	assert.Equal(t, err.Error(), "nil log level")

	// Invalid values
	var logLevel = LogLevel(math.MaxUint32)
	text, err := logLevel.MarshalText()
	assert.Error(t, err, nil)
	assert.Empty(t, string(text))

	logLevel = LevelNone
	err = logLevel.UnmarshalText([]byte(""))
	assert.Error(t, err, nil)
	assert.Equal(t, LevelNone, logLevel)
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

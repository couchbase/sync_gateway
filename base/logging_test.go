//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/couchbase/goutils/logging"
	"github.com/natefinch/lumberjack"
	"github.com/stretchr/testify/assert"
)

// asserts that the logs produced by function f contain string s.
func assertLogContains(t *testing.T, s string, f func()) {
	originalLogger := consoleLogger
	b := bytes.Buffer{}

	// temporarily override logger for the function call
	level := LevelDebug
	consoleLogger = &ConsoleLogger{LogLevel: &level, FileLogger: FileLogger{Enabled: true, logger: log.New(&b, "", 0)}}
	defer func() { consoleLogger = originalLogger }()

	f()
	assert.Contains(t, b.String(), s)
}

func TestRedactedLogFuncs(t *testing.T) {
	username := UD("alice")

	defer func() { RedactUserData = false }()

	RedactUserData = false
	assertLogContains(t, "Username: alice", func() { Infof(KeyAll, "Username: %s", username) })
	RedactUserData = true
	assertLogContains(t, "Username: <ud>alice</ud>", func() { Infof(KeyAll, "Username: %s", username) })

	RedactUserData = false
	assertLogContains(t, "Username: alice", func() { Warnf("Username: %s", username) })
	RedactUserData = true
	assertLogContains(t, "Username: <ud>alice</ud>", func() { Warnf("Username: %s", username) })
}

func Benchmark_LoggingPerformance(b *testing.B) {

	consoleLogger.LogKey.Enable(KeyCRUD)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Debugf(KeyCRUD, "some crud'y message")
		Infof(KeyCRUD, "some crud'y message")
		Warnf("some crud'y message")
		Errorf("some crud'y message")
	}
}

// func PrependContextID(contextID, format string, params ...interface{}) (newFormat string, newParams []interface{}) {

func TestPrependContextID(t *testing.T) {

	contextID := "context"

	var testInputsOutputs = []struct {
		inputFormat  string        // input
		inputParams  []interface{} // input
		outputFormat string        // output
		outputParams []interface{} // outout
	}{
		{
			"%v",
			[]interface{}{"hello"},
			"[%s] %v",
			[]interface{}{contextID, "hello"},
		},
		{
			"",
			[]interface{}{},
			"[%s] ",
			[]interface{}{contextID},
		},
	}

	for _, testInputOutput := range testInputsOutputs {
		newFormat, newParams := PrependContextID(contextID, testInputOutput.inputFormat, testInputOutput.inputParams...)
		assert.Equal(t, testInputOutput.outputFormat, newFormat)

		assert.Equal(t, len(testInputOutput.outputParams), len(newParams))
		for i, newParam := range newParams {
			assert.Equal(t, testInputOutput.outputParams[i], newParam)
		}
	}

	log.Printf("testInputsOutputs: %+v", testInputsOutputs)
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
			logPath := filepath.Join(os.TempDir(), "benchmark-logrotate")
			logger := lumberjack.Logger{Filename: filepath.Join(logPath, "output.log"), Compress: test.compress}
			defer logger.Close()
			defer os.RemoveAll(logPath)

			data := make([]byte, test.numBytes)
			_, err := rand.Read(data)
			if err != nil {
				bm.Error(err)
			}

			bm.ResetTimer()
			for i := 0; i < bm.N; i++ {
				_, _ = logger.Write(data)
				if test.rotate {
					_ = logger.Rotate()
				}
			}
		})
	}

}

func TestLoggingLevel(t *testing.T) {
	level := DebugLevel
	assert.Equal(t, logging.Level(0x8), level.cgLevel())
	assert.Equal(t, logging.DEBUG, level.cgLevel())
	assert.Equal(t, "debug", level.String())
	bytes, err := level.MarshalText()
	assert.Equal(t, []byte(level.String()), bytes)
	assert.NoError(t, err, "No error while marshalling debug logging level")
	assert.NoError(t, level.UnmarshalText([]byte(level.String())), level.String())
	assert.Equal(t, level.String(), ToDeprecatedLogLevel(LevelDebug).String())
	assert.Equal(t, level.String(), ToLogLevel(*(ToDeprecatedLogLevel(LevelDebug))).String())

	level = InfoLevel
	assert.Equal(t, logging.Level(0x5), level.cgLevel())
	assert.Equal(t, logging.INFO, level.cgLevel())
	assert.Equal(t, "info", level.String())
	bytes, err = level.MarshalText()
	assert.Equal(t, []byte(level.String()), bytes)
	assert.NoError(t, err, "No error while marshalling info logging level")
	assert.NoError(t, level.UnmarshalText([]byte(level.String())), level.String())
	assert.Equal(t, level.String(), ToDeprecatedLogLevel(LevelInfo).String())
	assert.Equal(t, level.String(), ToLogLevel(*(ToDeprecatedLogLevel(LevelInfo))).String())

	level = WarnLevel
	assert.Equal(t, logging.Level(0x4), level.cgLevel())
	assert.Equal(t, logging.WARN, level.cgLevel())
	assert.Equal(t, "warn", level.String())
	bytes, err = level.MarshalText()
	assert.Equal(t, []byte(level.String()), bytes)
	assert.NoError(t, err, "No error while marshalling warn logging level")
	assert.NoError(t, level.UnmarshalText([]byte(level.String())), level.String())
	assert.Equal(t, level.String(), ToDeprecatedLogLevel(LevelWarn).String())
	assert.Equal(t, level.String(), ToLogLevel(*(ToDeprecatedLogLevel(LevelWarn))).String())

	level = ErrorLevel
	assert.Equal(t, logging.Level(0x3), level.cgLevel())
	assert.Equal(t, logging.ERROR, level.cgLevel())
	assert.Equal(t, "error", level.String())
	bytes, err = level.MarshalText()
	assert.Equal(t, []byte(level.String()), bytes)
	assert.NoError(t, err, "No error while marshalling error logging level")
	assert.NoError(t, level.UnmarshalText([]byte(level.String())), level.String())
	assert.Equal(t, level.String(), ToDeprecatedLogLevel(LevelError).String())
	assert.Equal(t, level.String(), ToLogLevel(*(ToDeprecatedLogLevel(LevelError))).String())

	level = PanicLevel
	assert.Equal(t, logging.Level(0x2), level.cgLevel())
	assert.Equal(t, logging.SEVERE, level.cgLevel())
	assert.Equal(t, "panic", level.String())
	bytes, err = level.MarshalText()
	assert.Equal(t, []byte(level.String()), bytes)
	assert.NoError(t, err, "No error while marshalling panic logging level")
	assert.NoError(t, level.UnmarshalText([]byte(level.String())), level.String())

	level = FatalLevel
	assert.Equal(t, logging.Level(0x2), level.cgLevel())
	assert.Equal(t, logging.SEVERE, level.cgLevel())
	assert.Equal(t, "fatal", level.String())
	bytes, err = level.MarshalText()
	assert.Equal(t, []byte(level.String()), bytes)
	assert.NoError(t, err, "No error while marshalling fatal logging level")
	assert.NoError(t, level.UnmarshalText([]byte(level.String())), level.String())

	level = Level(0x5)
	assert.Equal(t, logging.NONE, level.cgLevel())
	assert.Equal(t, "Level(5)", level.String())
	bytes, err = level.MarshalText()
	assert.Equal(t, []byte(level.String()), bytes)
	assert.NoError(t, err, "No error while marshalling unknown logging level")
	assert.Error(t, level.UnmarshalText([]byte(level.String())), level.String())
}

func TestLogColor(t *testing.T) {
	consoleLogger.ColorEnabled = true

	if colorEnabled() {
		assert.Equal(t, "\x1b[0;36mFormat\x1b[0m", color("Format", LevelDebug))
		assert.Equal(t, "\x1b[1;34mFormat\x1b[0m", color("Format", LevelInfo))
		assert.Equal(t, "\x1b[1;33mFormat\x1b[0m", color("Format", LevelWarn))
		assert.Equal(t, "\x1b[1;31mFormat\x1b[0m", color("Format", LevelError))
		assert.Equal(t, "\x1b[0;37mFormat\x1b[0m", color("Format", LevelTrace))
		assert.Equal(t, "\x1b[0mFormat\x1b[0m", color("Format", LevelNone))
	}

	consoleLogger.ColorEnabled = false
	assert.Equal(t, "Format", color("Format", LevelDebug))
	assert.Equal(t, "Format", color("Format", LevelInfo))
	assert.Equal(t, "Format", color("Format", LevelWarn))
	assert.Equal(t, "Format", color("Format", LevelError))
	assert.Equal(t, "Format", color("Format", LevelTrace))
	assert.Equal(t, "Format", color("Format", LevelNone))
}

func TestSGLevel(t *testing.T) {
	assert.Equal(t, 1, DebugLevel.sgLevel())
	assert.Equal(t, 1, InfoLevel.sgLevel())
	assert.Equal(t, 2, WarnLevel.sgLevel())
	assert.Equal(t, 2, ErrorLevel.sgLevel())
	assert.Equal(t, 3, PanicLevel.sgLevel())
	assert.Equal(t, 3, FatalLevel.sgLevel())
	assert.Equal(t, 4, Level(5).sgLevel())
}

func TestMarshalTextError(t *testing.T) {
	var level *Level
	bytes, err := level.MarshalText()
	assert.Nil(t, bytes, "bytes should be nil")
	assert.Error(t, err, "Can't marshal a nil *Level to text")
	assert.Equal(t, err.Error(), "can't marshal a nil *Level to text")
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

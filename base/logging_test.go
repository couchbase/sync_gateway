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
	"testing"

	"github.com/couchbaselabs/go.assert"
	"github.com/natefinch/lumberjack"
)

// asserts that the logs produced by function f contain string s.
func assertLogContains(t *testing.T, s string, f func()) {
	originalLogger := consoleLogger
	b := bytes.Buffer{}

	// temporarily override logger for the function call
	level := LevelDebug
	consoleLogger = &ConsoleLogger{LogLevel: &level, logger: log.New(&b, "", 0)}
	defer func() { consoleLogger = originalLogger }()

	f()
	assert.StringContains(t, b.String(), s)
}

func TestRedactedLogFuncs(t *testing.T) {
	username := UD("alice")

	defer func() { RedactUserData = false }()

	RedactUserData = false
	assertLogContains(t, "Username: alice", func() { Infof(KeyAll, "Username: %s", username) })
	RedactUserData = true
	assertLogContains(t, "Username: <ud>alice</ud>", func() { Infof(KeyAll, "Username: %s", username) })

	RedactUserData = false
	assertLogContains(t, "Username: alice", func() { Warnf(KeyAll, "Username: %s", username) })
	RedactUserData = true
	assertLogContains(t, "Username: <ud>alice</ud>", func() { Warnf(KeyAll, "Username: %s", username) })
}

func Benchmark_LoggingPerformance(b *testing.B) {

	consoleLogger.LogKey.Enable(KeyCRUD)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Debugf(KeyCRUD, "some crud'y message")
		Infof(KeyCRUD, "some crud'y message")
		Warnf(KeyCRUD, "some crud'y message")
		Errorf(KeyCRUD, "some crud'y message")
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
		assert.Equals(t, newFormat, testInputOutput.outputFormat)

		assert.Equals(t, len(newParams), len(testInputOutput.outputParams))
		for i, newParam := range newParams {
			assert.Equals(t, newParam, testInputOutput.outputParams[i])
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
		b.Run(fmt.Sprintf("rotate:%t-compress:%t-Bytes:%v", test.rotate, test.compress, test.numBytes), func(bm *testing.B) {
			logger := lumberjack.Logger{Compress: test.compress}

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

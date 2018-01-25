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
	"errors"
	"github.com/couchbaselabs/go.assert"
	"log"
	"testing"
)

func Benchmark_LoggingPerformance(b *testing.B) {

	var logKeys = map[string]bool{
		"CRUD": true,
	}

	UpdateLogKeys(logKeys, true)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		LogTo("CRUD", "some crud'y message")
		Log("An unformatted log message")
		Logf("%s", "A formatted log message")
		LogError(errors.New("This is an error message"))
		Warn("%s", "A WARNING message")
		TEMP("%s", "A TEMP message")
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

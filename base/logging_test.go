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

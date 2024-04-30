// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// SkipTestMain logs in the same format as actual test output that we're skipping the current package.
func SkipTestMain(m *testing.M, format string, args ...interface{}) {
	fmt.Println("=== RUN   TestMain")
	printfFromLine(2, format+"\n", args...)
	fmt.Println("--- SKIP: TestMain (0.00s)")
	os.Exit(0)
}

// printfFromLine prints the given message with the filename and line number from the caller
func printfFromLine(skip int, format string, args ...interface{}) {
	_, filename, line, _ := runtime.Caller(skip)
	filename = filepath.Base(filename) // trim
	args = append([]interface{}{filename, line}, args...)
	// E.g: main_test.go:25: msg
	fmt.Printf("%s:%d: "+format, args...)
}

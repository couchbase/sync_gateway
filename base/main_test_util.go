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

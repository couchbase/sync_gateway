// +build !windows,!darwin

package base

import "path/filepath"

// default log file path directory
var defaultLogFilePath = getLogFilePath()

// getLogFilePath returns the absolute path to place log files into.
func getLogFilePath() string {
	logFilePath := filepath.Join(
		filepath.Separator, "opt", "Couchbase",
		"var", "lib", "sync_gateway", "logs",
	)

	logFilePath, err := filepath.Abs(logFilePath)
	if err != nil {
		panic(err)
	}

	return logFilePath
}

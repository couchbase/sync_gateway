package base

import (
	"os"
	"path/filepath"
)

// default log file path directory
var defaultLogFilePath = getLogFilePath()

// getLogFilePath returns the absolute path to place log files into.
func getLogFilePath() string {
	home := "~"

	if homeEnv := os.Getenv("HOME"); homeEnv != "" {
		home = filepath.FromSlash(homeEnv)
	}

	logFilePath := filepath.Join(
		home,
		"Library",
		"Application Support",
		"sync_gateway",
		"var",
		"lib",
		"sync_gateway",
		"logs",
	)

	logFilePath, err := filepath.Abs(logFilePath)
	if err != nil {
		panic(err)
	}

	return logFilePath
}

package base

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// LogLevel is used to represent a log level.
type LogLevel uint32

const (
	// LEVEL_NONE disables all logging
	LEVEL_NONE LogLevel = iota
	// LEVEL_ERROR enables only error logging.
	LEVEL_ERROR
	// LEVEL_WARN enables warn, and error logging.
	LEVEL_WARN
	// LEVEL_INFO enables info, warn, and error logging.
	LEVEL_INFO
	// LEVEL_DEBUG enables all logging.
	LEVEL_DEBUG
)

var logLevelNames = []string{"none", "error", "warn", "info", "debug"}

func (l *LogLevel) Set(newLevel LogLevel) {
	atomic.StoreUint32((*uint32)(l), uint32(newLevel))
}

// Enabled returns true if the log level is enabled.
func (l *LogLevel) Enabled(logLevel LogLevel) bool {
	if l == nil {
		return false
	}
	return atomic.LoadUint32((*uint32)(l)) >= uint32(logLevel)
}

// LogLevelName returns the string representation of a log level.
func LogLevelName(logLevel LogLevel) string {
	// No lock required to read concurrently, as long as nobody writes to logLevelNames.
	return logLevelNames[logLevel]
}

func (l *LogLevel) MarshalText() (text []byte, err error) {
	if l == nil {
		return nil, errors.New("invalid log level")
	}
	return []byte(LogLevelName(*l)), nil
}

func (l *LogLevel) UnmarshalText(text []byte) error {
	for i, name := range logLevelNames {
		if name == string(text) {
			*l = LogLevel(i)
			return nil
		}
	}
	return fmt.Errorf("unrecognized log level: %q (valid options: %v)", string(text), logLevelNames)
}

package base

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// LogLevel is used to represent a log level.
type LogLevel uint32

const (
	// LevelNone disables all logging
	LevelNone LogLevel = iota
	// LevelError enables only error logging.
	LevelError
	// LevelWarn enables warn, and error logging.
	LevelWarn
	// LevelInfo enables info, warn, and error logging.
	LevelInfo
	// LevelDebug enables all logging.
	LevelDebug

	levelCount
)

var (
	logLevelNames      = []string{"none", "error", "warn", "info", "debug"}
	logLevelNamesPrint = []string{"NON", "ERR", "WRN", "INF", "DBG"}
)

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

// String returns the string representation of a log level (e.g. "debug" or "warn")
func (l LogLevel) String() string {
	if int(l) >= len(logLevelNames) {
		return ""
	}
	return logLevelNames[l]
}

// StringShort returns the short string representation of a log level (e.g. "DBG" or "WRN")
func (l LogLevel) StringShort() string {
	if int(l) >= len(logLevelNames) {
		return ""
	}
	return logLevelNamesPrint[l]
}

func (l *LogLevel) MarshalText() (text []byte, err error) {
	if l == nil {
		return nil, errors.New("invalid log level")
	}
	name := l.String()
	if name == "" {
		return nil, fmt.Errorf("unrecognized log level: %v (valid range: %d-%d)", *l, 0, levelCount-1)
	}
	return []byte(name), nil
}

func (l *LogLevel) UnmarshalText(text []byte) error {
	if l == nil {
		return errors.New("invalid log level")
	}
	for i, name := range logLevelNames {
		if name == string(text) {
			l.Set(LogLevel(i))
			return nil
		}
	}
	return fmt.Errorf("unrecognized log level: %q (valid options: %v)", string(text), logLevelNames)
}

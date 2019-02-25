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
	// LevelDebug enables debug, info, warn, and error logging.
	LevelDebug
	// LevelTrace enables trace, debug, info, warn, and error logging logging.
	LevelTrace

	levelCount
)

var (
	logLevelNames      = []string{"none", "error", "warn", "info", "debug", "trace"}
	logLevelNamesPrint = []string{"---", "ERR", "WRN", "INF", "DBG", "TRC"}
)

// Set will override the log level with the given log level.
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
	if l >= levelCount {
		return fmt.Sprintf("LogLevel(%d)", l)

	}
	return logLevelNames[l]
}

// StringShort returns the short string representation of a log level (e.g. "DBG" or "WRN")
func (l LogLevel) StringShort() string {
	if l >= levelCount {
		return fmt.Sprintf("LVL(%d)", l)
	}
	return logLevelNamesPrint[l]
}

// MarshalText implements the TextMarshaler interface.
func (l *LogLevel) MarshalText() (text []byte, err error) {
	if l == nil || *l >= levelCount {
		return nil, fmt.Errorf("unrecognized log level: %v (valid range: %d-%d)", l, 0, levelCount-1)
	}
	return []byte(l.String()), nil
}

// UnmarshalText implements the TextUnmarshaler interface.
func (l *LogLevel) UnmarshalText(text []byte) error {
	if l == nil {
		return errors.New("nil log level")
	}
	for i, name := range logLevelNames {
		if name == string(text) {
			l.Set(LogLevel(i))
			return nil
		}
	}
	return fmt.Errorf("unrecognized log level: %q (valid options: %v)", string(text), logLevelNames)
}

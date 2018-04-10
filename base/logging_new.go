package base

import (
	"log"
	"os"
	"time"

	"github.com/pkg/errors"
)

const (
	// RFC3339 + 3 digit milli
	timestampFormat = "2006-01-02T15:04:05.000Z07:00"
)

var (
	ErrInvalidLogFilePath   = errors.New("invalid LogFilePath")
	ErrInvalidLoggingMaxAge = errors.New("invalid MaxAge")

	// We'll initilise a default consoleLogger so we can still log stuff before/during parsing logging configs.
	defaultLogLevel = LevelInfo
	defaultLogKey   = KeyAll
	consoleLogger   = LogConsoleConfig{
		LogLevel:     &defaultLogLevel,
		logKey:       &defaultLogKey,
		logger:       log.New(os.Stderr, "", 0),
		ColorEnabled: true,
	}

	debugLogger, infoLogger, warnLogger, errorLogger LogFileConfig
)

// Errorf logs the given formatted string and args to the error log level and given log key.
func Errorf(logKey LogKey, format string, args ...interface{}) {
	logTo(LevelError, logKey, format, args...)
}

// Warnf logs the given formatted string and args to the warn log level and given log key.
func Warnf(logKey LogKey, format string, args ...interface{}) {
	logTo(LevelWarn, logKey, format, args...)
}

// Infof logs the given formatted string and args to the info log level and given log key.
func Infof(logKey LogKey, format string, args ...interface{}) {
	logTo(LevelInfo, logKey, format, args...)
}

// Debugf logs the given formatted string and args to the debug log level with an optional log key.
func Debugf(logKey LogKey, format string, args ...interface{}) {
	logTo(LevelDebug, logKey, format, args...)
}

func logTo(logLevel LogLevel, logKey LogKey, format string, args ...interface{}) {
	// exit early for things we know won't get logged
	if logKey <= KeyNone || logLevel <= LevelNone {
		return
	}

	format = addPrefixes(format, logLevel, logKey)

	// Perform log redaction, if necessary.
	args = redact(args)

	if consoleLogger.shouldLog(logLevel, logKey) {
		consoleLogger.logger.Printf(color(format, logLevel), args...)
	}

	switch logLevel {
	case LevelError:
		if errorLogger.shouldLog() {
			errorLogger.logger.Printf(format, args...)
		}
		fallthrough
	case LevelWarn:
		if warnLogger.shouldLog() {
			warnLogger.logger.Printf(format, args...)
		}
		fallthrough
	case LevelInfo:
		if infoLogger.shouldLog() {
			infoLogger.logger.Printf(format, args...)
		}
		fallthrough
	case LevelDebug:
		if debugLogger.shouldLog() {
			debugLogger.logger.Printf(format, args...)
		}
	}
}

// Broadcastf will print the same log to ALL outputs, ignoring logLevel and logKey settings.
// This can be useful for printing an indicator of app restarts, version numbers, etc. but MUST be used sparingly.
func Broadcastf(format string, args ...interface{}) {
	format = addPrefixes(format, LevelNone, KeyNone)
	if consoleLogger.logger != nil {
		consoleLogger.logger.Printf(color(format, LevelNone), args...)
	}
	if errorLogger.shouldLog() {
		errorLogger.logger.Printf(format, args...)
	}
	if warnLogger.shouldLog() {
		warnLogger.logger.Printf(format, args...)
	}
	if infoLogger.shouldLog() {
		infoLogger.logger.Printf(format, args...)
	}
	if debugLogger.shouldLog() {
		debugLogger.logger.Printf(format, args...)
	}
}

// addPrefixes will modify the format string to add timestamps, log level, and other common prefixes.
func addPrefixes(format string, logLevel LogLevel, logKey LogKey) string {
	timestampPrefix := time.Now().Format(timestampFormat) + " "

	var logLevelPrefix string
	if logLevel > LevelNone {
		logLevelPrefix = "[" + logLevelNamePrint(logLevel) + "] "
	}

	var logKeyPrefix string
	if logKey > KeyNone && logKey != KeyAll {
		logKeyName := LogKeyName(logKey)
		// Append "+" to logKeys at debug level (for backwards compatibility)
		if logLevel == LevelDebug {
			logKeyName += "+"
		}
		logKeyPrefix = logKeyName + ": "
	}

	return timestampPrefix + logLevelPrefix + logKeyPrefix + format
}

// color is a stub that can be used in the future to color based on log level
func color(str string, logLevel LogLevel) string {
	return str
}

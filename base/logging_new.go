package base

import (
	"log"
	"os"
	"time"

	"github.com/pkg/errors"
)

var (
	ErrInvalidLogFilePath   = errors.New("Invalid LogFilePath")
	ErrInvalidLoggingMaxAge = errors.New("Invalid MaxAge")

	// We'll initilise a default consoleLogger so we can still log stuff before/during parsing logging configs.
	// This maintains consistent formatting (timestamps, levels, etc) in the output,
	// and allows a single set of logging functions to be used, rather than fmt.Printf()
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
	shouldLogConsole := consoleLogger.shouldLog(logLevel, logKey)
	shouldLogError := errorLogger.shouldLog()
	shouldLogWarn := warnLogger.shouldLog()
	shouldLogInfo := infoLogger.shouldLog()
	shouldLogDebug := debugLogger.shouldLog()

	shouldLog := shouldLogConsole || shouldLogError || shouldLogWarn || shouldLogInfo || shouldLogDebug

	// exit early if we aren't going to log anything
	if !shouldLog || logLevel <= LevelNone {
		return
	}

	// Prepend timestamp, level, log key
	format = addPrefixes(format, logLevel, logKey)

	// Perform log redaction, if necessary.
	args = redact(args)

	if shouldLogConsole {
		consoleLogger.logger.Printf(color(format, logLevel), args...)
	}

	switch logLevel {
	case LevelError:
		if shouldLogError {
			errorLogger.logger.Printf(format, args...)
		}
		fallthrough
	case LevelWarn:
		if shouldLogWarn {
			warnLogger.logger.Printf(format, args...)
		}
		fallthrough
	case LevelInfo:
		if shouldLogInfo {
			infoLogger.logger.Printf(format, args...)
		}
		fallthrough
	case LevelDebug:
		if shouldLogDebug {
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
	timestampPrefix := time.Now().Format(ISO8601Format) + " "

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

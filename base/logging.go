//  Copyright (c) 2012-2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"fmt"
	"os"
	"runtime"
	"time"
)

var (
	consoleLogger                                    *ConsoleLogger
	debugLogger, infoLogger, warnLogger, errorLogger, statsLogger *FileLogger
)

// RotateLogfiles rotates all active log files.
func RotateLogfiles() map[*FileLogger]error {
	Infof(KeyAll, "Rotating log files...")

	loggers := map[*FileLogger]error{
		debugLogger: nil,
		infoLogger:  nil,
		warnLogger:  nil,
		errorLogger: nil,
		statsLogger: nil,
	}

	for logger := range loggers {
		loggers[logger] = logger.Rotate()
	}

	return loggers
}

func init() {
	// We'll initilise a default consoleLogger so we can still log stuff before/during parsing logging configs.
	// This maintains consistent formatting (timestamps, levels, etc) in the output,
	// and allows a single set of logging functions to be used, rather than fmt.Printf()

	// initialCollationBufferSize is set to zero for disabling log collation before
	// initializing a logging config, and when running under a test scenario.
	initialCollationBufferSize := 0

	consoleLogger = newConsoleLoggerOrPanic(&ConsoleLoggerConfig{CollationBufferSize: &initialCollationBufferSize})
	initExternalLoggers()
}

// Panicf logs the given formatted string and args to the error log level and given log key and then panics.
func Panicf(logKey LogKey, format string, args ...interface{}) {
	logTo(LevelError, logKey, format, args...)
	panic(fmt.Sprintf(format, args...))
}

// Fatalf logs the given formatted string and args to the error log level and given log key and then exits.
func Fatalf(logKey LogKey, format string, args ...interface{}) {
	logTo(LevelError, logKey, format, args...)
	FlushLogBuffers()
	os.Exit(1)
}

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

// Tracef logs the given formatted string and args to the trace log level with an optional log key.
func Tracef(logKey LogKey, format string, args ...interface{}) {
	logTo(LevelTrace, logKey, format, args...)
}

func Statsf(stats []byte) {
	statsLogger.logf("%s", string(stats))
}

func logTo(logLevel LogLevel, logKey LogKey, format string, args ...interface{}) {
	// Defensive bounds-check for log level. All callers of this funcion should be within this range.
	if logLevel <= LevelNone || logLevel >= levelCount {
		return
	}

	shouldLogConsole := consoleLogger.shouldLog(logLevel, logKey)
	shouldLogError := errorLogger.shouldLog(logLevel)
	shouldLogWarn := warnLogger.shouldLog(logLevel)
	shouldLogInfo := infoLogger.shouldLog(logLevel)
	shouldLogDebug := debugLogger.shouldLog(logLevel)

	// exit early if we aren't going to log anything anywhere.
	if !(shouldLogConsole || shouldLogError || shouldLogWarn || shouldLogInfo || shouldLogDebug) {
		return
	}

	// Prepend timestamp, level, log key.
	format = addPrefixes(format, logLevel, logKey)

	// Warn and error logs also append caller name/line numbers.
	if logLevel <= LevelWarn {
		format += " -- " + GetCallersName(2, true)
	}

	// Perform log redaction, if necessary.
	args = redact(args)

	if shouldLogConsole {
		consoleLogger.logf(color(format, logLevel), args...)
	}
	if shouldLogError {
		errorLogger.logf(format, args...)
	}
	if shouldLogWarn {
		warnLogger.logf(format, args...)
	}
	if shouldLogInfo {
		infoLogger.logf(format, args...)
	}
	if shouldLogDebug {
		debugLogger.logf(format, args...)
	}
}

// LogSyncGatewayVersion will print the startup indicator and version number to all log outputs.
func LogSyncGatewayVersion() {
	format := addPrefixes("==== %s ====", LevelNone, KeyNone)
	msg := fmt.Sprintf(format, LongVersionString)

	if consoleLogger.logger != nil {
		consoleLogger.logger.Print(color(msg, LevelNone))
	}
	if errorLogger.shouldLog(LevelNone) {
		errorLogger.logger.Printf(msg)
	}
	if warnLogger.shouldLog(LevelNone) {
		warnLogger.logger.Printf(msg)
	}
	if infoLogger.shouldLog(LevelNone) {
		infoLogger.logger.Printf(msg)
	}
	if debugLogger.shouldLog(LevelNone) {
		debugLogger.logger.Printf(msg)
	}
}

// addPrefixes will modify the format string to add timestamps, log level, and other common prefixes.
func addPrefixes(format string, logLevel LogLevel, logKey LogKey) string {
	timestampPrefix := time.Now().Format(ISO8601Format) + " "

	var logLevelPrefix string
	if logLevel > LevelNone {
		logLevelPrefix = "[" + logLevel.StringShort() + "] "
	}

	var logKeyPrefix string
	if logKey > KeyNone && logKey != KeyAll {
		logKeyName := logKey.String()
		// Append "+" to logKeys at debug level (for backwards compatibility)
		if logLevel == LevelDebug {
			logKeyName += "+"
		}
		logKeyPrefix = logKeyName + ": "
	}

	return timestampPrefix + logLevelPrefix + logKeyPrefix + format
}

// color wraps the given string with color based on logLevel
// This won't work on Windows. Maybe use fatih's colour package?
func color(str string, logLevel LogLevel) string {
	if !colorEnabled() {
		return str
	}

	var color string

	switch logLevel {
	case LevelError:
		color = "\033[1;31m" // Red
	case LevelWarn:
		color = "\033[1;33m" // Yellow
	case LevelInfo:
		color = "\033[1;34m" // Blue
	case LevelDebug:
		color = "\033[0;36m" // Cyan
	case LevelTrace:
		color = "\033[0;37m" // White
	default:
		color = "\033[0m" // None
	}

	return color + str + "\033[0m"
}

func colorEnabled() bool {
	return consoleLogger.ColorEnabled &&
		os.Getenv("TERM") != "dumb" &&
		runtime.GOOS != "windows"
}

// ConsoleLogLevel returns the console log level.
func ConsoleLogLevel() *LogLevel {
	return consoleLogger.LogLevel
}

// ConsoleLogKey returns the console log key.
func ConsoleLogKey() *LogKey {
	return consoleLogger.LogKey
}

// LogInfoEnabled returns true if either the console should log at info level,
// or if the infoLogger is enabled.
func LogInfoEnabled(logKey LogKey) bool {
	return consoleLogger.shouldLog(LevelInfo, logKey) || infoLogger.shouldLog(LevelInfo)
}

// LogDebugEnabled returns true if either the console should log at debug level,
// or if the debugLogger is enabled.
func LogDebugEnabled(logKey LogKey) bool {
	return consoleLogger.shouldLog(LevelDebug, logKey) || debugLogger.shouldLog(LevelDebug)
}

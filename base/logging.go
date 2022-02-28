//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"time"
)

// GetLogKeys returns log keys in a map
func GetLogKeys() map[string]bool {
	consoleLogKeys := ConsoleLogKey().EnabledLogKeys()
	logKeys := make(map[string]bool, len(consoleLogKeys))
	for _, v := range consoleLogKeys {
		logKeys[v] = true
	}
	return logKeys
}

// UpdateLogKeys updates the console's log keys from a map
func UpdateLogKeys(keys map[string]bool, replace bool) {
	if replace {
		ConsoleLogKey().Set(logKeyMask(KeyNone))
	}

	for k, v := range keys {
		key := strings.Replace(k, "+", "", -1)
		if v {
			ConsoleLogKey().Enable(logKeyNamesInverse[key])
		} else {
			ConsoleLogKey().Disable(logKeyNamesInverse[key])
		}
	}

	InfofCtx(context.Background(), KeyAll, "Setting log keys to: %v", ConsoleLogKey().EnabledLogKeys())
}

// Returns a string identifying a function on the call stack.
// Use depth=1 for the caller of the function that calls GetCallersName, etc.
func GetCallersName(depth int, includeLine bool) string {
	pc, file, line, ok := runtime.Caller(depth + 1)
	if !ok {
		return "???"
	}

	fnname := ""
	if fn := runtime.FuncForPC(pc); fn != nil {
		fnname = lastComponent(fn.Name())
	}

	if !includeLine {
		return fnname
	}

	return fmt.Sprintf("%s() at %s:%d", fnname, lastComponent(file), line)
}

func lastComponent(path string) string {
	if index := strings.LastIndex(path, "/"); index >= 0 {
		path = path[index+1:]
	} else if index = strings.LastIndex(path, "\\"); index >= 0 {
		path = path[index+1:]
	}
	return path
}

// *************************************************************************
//   2018-04-10: New logging below
// *************************************************************************

var (
	consoleLogger                                                              *ConsoleLogger
	traceLogger, debugLogger, infoLogger, warnLogger, errorLogger, statsLogger *FileLogger

	// envColorCapable evaluated only once to prevent unnecessary
	// overhead of checking os.Getenv on each colorEnabled() invocation
	envColorCapable = runtime.GOOS != "windows" && os.Getenv("TERM") != "dumb"
)

// RotateLogfiles rotates all active log files.
func RotateLogfiles() map[*FileLogger]error {
	InfofCtx(context.Background(), KeyAll, "Rotating log files...")

	loggers := map[*FileLogger]error{
		traceLogger: nil,
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

	consoleLogger = mustInitConsoleLogger(&ConsoleLoggerConfig{FileLoggerConfig: FileLoggerConfig{Enabled: BoolPtr(true), CollationBufferSize: &initialCollationBufferSize}})
	initExternalLoggers()
}

// PanicfCtx logs the given formatted string and args to the error log level and given log key and then panics.
func PanicfCtx(ctx context.Context, format string, args ...interface{}) {
	// Fall back to stdlib's log.Panicf if SG loggers aren't set up.
	if errorLogger == nil {
		log.Panicf(format, args...)
	}
	logTo(ctx, LevelError, KeyAll, format, args...)
	FlushLogBuffers()
	panic(fmt.Sprintf(format, args...))
}

// FatalfCtx logs the given formatted string and args to the error log level and given log key and then exits.
func FatalfCtx(ctx context.Context, format string, args ...interface{}) {
	// Fall back to stdlib's log.Panicf if SG loggers aren't set up.
	if errorLogger == nil {
		log.Fatalf(format, args...)
	}
	logTo(ctx, LevelError, KeyAll, format, args...)
	FlushLogBuffers()
	os.Exit(1)
}

// ErrorfCtx logs the given formatted string and args to the error log level and given log key.
func ErrorfCtx(ctx context.Context, format string, args ...interface{}) {
	logTo(ctx, LevelError, KeyAll, format, args...)
}

// WarnfCtx logs the given formatted string and args to the warn log level and given log key.
func WarnfCtx(ctx context.Context, format string, args ...interface{}) {
	logTo(ctx, LevelWarn, KeyAll, format, args...)
}

// InfofCtx logs the given formatted string and args to the info log level and given log key.
func InfofCtx(ctx context.Context, logKey LogKey, format string, args ...interface{}) {
	logTo(ctx, LevelInfo, logKey, format, args...)
}

// DebugfCtx logs the given formatted string and args to the debug log level with an optional log key.
func DebugfCtx(ctx context.Context, logKey LogKey, format string, args ...interface{}) {
	logTo(ctx, LevelDebug, logKey, format, args...)
}

// TracefCtx logs the given formatted string and args to the trace log level with an optional log key.
func TracefCtx(ctx context.Context, logKey LogKey, format string, args ...interface{}) {
	logTo(ctx, LevelTrace, logKey, format, args...)
}

// RecordStats writes the given stats JSON content to a stats log file, if enabled.
// The content passed in is expected to be a JSON dictionary.
func RecordStats(statsJson string) {
	if statsLogger != nil {
		statsLogger.logf(statsJson)
	}
}

// logTo is the "core" logging function. All other logging functions (like Debugf(), WarnfCtx(), etc.) end up here.
// The function will fan out the log to all of the various outputs for them to decide if they should log it or not.
func logTo(ctx context.Context, logLevel LogLevel, logKey LogKey, format string, args ...interface{}) {
	// Defensive bounds-check for log level. All callers of this function should be within this range.
	if logLevel < LevelNone || logLevel >= levelCount {
		return
	}

	if logLevel == LevelError {
		SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.Add(1)
	} else if logLevel == LevelWarn {
		SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Add(1)
	}

	shouldLogConsole := consoleLogger.shouldLog(logLevel, logKey)
	shouldLogError := errorLogger.shouldLog(logLevel)
	shouldLogWarn := warnLogger.shouldLog(logLevel)
	shouldLogInfo := infoLogger.shouldLog(logLevel)
	shouldLogDebug := debugLogger.shouldLog(logLevel)
	shouldLogTrace := traceLogger.shouldLog(logLevel)

	// exit early if we aren't going to log anything anywhere.
	if !(shouldLogConsole || shouldLogError || shouldLogWarn || shouldLogInfo || shouldLogDebug || shouldLogTrace) {
		return
	}

	// Prepend timestamp, level, log key.
	format = addPrefixes(format, ctx, logLevel, logKey)

	// Error, warn and trace logs also append caller name/line numbers.
	if logLevel <= LevelWarn || logLevel == LevelTrace {
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
	if shouldLogTrace {
		traceLogger.logf(format, args...)
	}
}

var consoleFOutput io.Writer = os.Stderr

// Consolef logs the given formatted string and args to the given log level and log key,
// as well as making sure the message is *always* logged to stdout.
func Consolef(logLevel LogLevel, logKey LogKey, format string, args ...interface{}) {
	logTo(context.Background(), logLevel, logKey, format, args...)

	// If the above logTo didn't already log to stderr, do it directly here
	if !consoleLogger.isStderr || !consoleLogger.shouldLog(logLevel, logKey) {
		format = color(addPrefixes(format, context.Background(), logLevel, logKey), logLevel)
		_, _ = fmt.Fprintf(consoleFOutput, format+"\n", args...)
	}
}

// LogSyncGatewayVersion will print the '==== name/version ====' startup indicator to ALL log outputs.
func LogSyncGatewayVersion() {
	msg := fmt.Sprintf("==== %s ====", LongVersionString)

	// Log the startup indicator to the stderr.
	Consolef(LevelNone, KeyNone, msg)

	// Log the startup indicator to ALL log files too.
	msg = addPrefixes(msg, context.Background(), LevelNone, KeyNone)
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
	if traceLogger.shouldLog(LevelNone) {
		traceLogger.logger.Printf(msg)
	}
}

// addPrefixes will modify the format string to add timestamps, log level, and other common prefixes.
// E.g: 2006-01-02T15:04:05.000Z07:00 [LVL] LogKeyMask: format_str
func addPrefixes(format string, ctx context.Context, logLevel LogLevel, logKey LogKey) string {
	timestampPrefix := time.Now().Format(ISO8601Format) + " "

	var logLevelPrefix string
	if logLevel > LevelNone {
		logLevelPrefix = "[" + logLevel.StringShort() + "] "
	}

	var logKeyPrefix string
	if logKey > KeyNone && logKey != KeyAll {
		logKeyName := logKey.String()
		// Append "+" to logKeys at debug level (for backwards compatibility)
		if logLevel >= LevelDebug {
			logKeyName += "+"
		}
		logKeyPrefix = logKeyName + ": "
	}

	if ctx != nil {
		if ctxVal := ctx.Value(LogContextKey{}); ctxVal != nil {
			if logCtx, ok := ctxVal.(LogContext); ok {
				format = logCtx.addContext(format)
			}
		}
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

// colorEnabled returns true if the console logger has color enabled,
// and the environment supports ANSI color escape sequences.
func colorEnabled() bool {
	return consoleLogger.ColorEnabled && envColorCapable
}

// ConsoleLogLevel returns the console log level.
func ConsoleLogLevel() *LogLevel {
	return consoleLogger.LogLevel
}

// ConsoleLogKey returns the console log key.
func ConsoleLogKey() *LogKeyMask {
	return consoleLogger.LogKeyMask
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

// LogTraceEnabled returns true if either the console should log at trace level,
// or if the traceLogger is enabled.
func LogTraceEnabled(logKey LogKey) bool {
	return consoleLogger.shouldLog(LevelTrace, logKey) || traceLogger.shouldLog(LevelTrace)
}

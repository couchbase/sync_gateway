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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/couchbase/goutils/logging"
)

var errMarshalNilLevel = errors.New("can't marshal a nil *Level to text")

const (
	// If true, all HTTP request/response bodies will be logged.
	// Use this sparingly as it will probably dump sensitive information into the logs.
	EnableLogHTTPBodies = false
)

type Level int32

// DeferredLogFn is an anonymous function that can be executed at a later date to log something.
type DeferredLogFn func()

//By setting DebugLevel to -1, if LogLevel is not set in the logging config it
//will default to the zero value for int32 (0) which will disable debug
//logging, InfoLevel logging will be the default output.
const (
	// DebugLevel logs are typically voluminous, and are usually disabled in
	// production.
	DebugLevel Level = iota - 1
	// InfoLevel is the default logging priority.
	InfoLevel
	// WarnLevel logs are more important than Info, but don't need individual
	// human review.
	WarnLevel
	// ErrorLevel logs are high-priority. If an application is running smoothly,
	// it shouldn't generate any error-level logs.
	ErrorLevel
	// PanicLevel logs a message, then panics.
	PanicLevel
	// FatalLevel logs a message, then calls os.Exit(1).
	FatalLevel
)

// cgLevel returns a compatible go-couchbase/golog Log Level for
// the given logging config Level
// The mapping is:
//
// DebugLevel	-1 --> 7
// InfoLevel 	 0 --> 4
// WarnLevel 	 1 --> 3
// ErrorLevel 	 2 --> 2
// PanicLevel 	 3 --> 1
// FatalLevel 	 4 --> 0
func (l Level) cgLevel() logging.Level {
	switch l {
	case DebugLevel:
		return logging.DEBUG
	case InfoLevel:
		return logging.INFO
	case WarnLevel:
		return logging.WARN
	case ErrorLevel:
		return logging.ERROR
	case PanicLevel:
		return logging.SEVERE
	case FatalLevel:
		return logging.SEVERE
	default:
		return logging.NONE
	}
}

// String returns a lower-case ASCII representation of the log level.
func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "debug"
	case InfoLevel:
		return "info"
	case WarnLevel:
		return "warn"
	case ErrorLevel:
		return "error"
	case PanicLevel:
		return "panic"
	case FatalLevel:
		return "fatal"
	default:
		return fmt.Sprintf("Level(%d)", l)
	}
}

// Implementaion of the go encoding.TextMarshaller interface for the Level type
// This method will also be called by the JSON Marshaller
//
// MarshalText marshals the Level to text. Note that the text representation
// drops the -Level suffix (see example).
func (l *Level) MarshalText() ([]byte, error) {
	if l == nil {
		return nil, errMarshalNilLevel
	}
	return []byte(l.String()), nil
}

// Implementaion of the go encoding.TextUnmarshaller interface for the Level type
// This method will also be called by the JSON Unmarshaller e.g. when loading
// from logging configuration.
//
// UnmarshalText unmarshals text to a level. Like MarshalText, UnmarshalText
// expects the text representation of a Level to drop the -Level suffix (see
// example).
//
// In particular, this makes it easy to configure logging levels using YAML,
// TOML, or JSON files.
func (l *Level) UnmarshalText(text []byte) error {
	switch string(text) {
	case "debug":
		*l = DebugLevel
	case "info":
		*l = InfoLevel
	case "warn":
		*l = WarnLevel
	case "error":
		*l = ErrorLevel
	case "panic":
		*l = PanicLevel
	case "fatal":
		*l = FatalLevel
	default:
		return fmt.Errorf("unrecognized level: %v", string(text))
	}
	return nil
}

type LogRotationConfig struct {
	// MaxSize is the maximum size in megabytes of the log file before it gets
	// rotated. It defaults to 100 megabytes.
	MaxSize int `json:",omitempty"`

	// MaxAge is the maximum number of days to retain old log files based on the
	// timestamp encoded in their filename.  Note that a day is defined as 24
	// hours and may not exactly correspond to calendar days due to daylight
	// savings, leap seconds, etc. The default is not to remove old log files
	// based on age.
	MaxAge int `json:",omitempty"`

	// MaxBackups is the maximum number of old log files to retain.  The default
	// is to retain all old log files (though MaxAge may still cause them to get
	// deleted.)
	MaxBackups int `json:",omitempty"`

	// LocalTime determines if the time used for formatting the timestamps in
	// backup files is the computer's local time.  The default is to use UTC
	// time.
	LocalTime bool `json:",omitempty"`
	// contains filtered or unexported fields
}

type LogAppenderConfig struct {
	// Filename is the file to write logs to.  Backup log files will be retained
	// in the same directory.  It uses <processname>-lumberjack.log in
	// os.TempDir() if empty.
	LogFilePath    *string            `json:",omitempty"`
	LogKeys        []string           `json:",omitempty"` // Log keywords to enable
	LogLevel       Level              `json:",omitempty"`
	Rotation       *LogRotationConfig `json:",omitempty"`
	RedactionLevel RedactionLevel     `json:",omitempty"`
}

// For transforming a new log level to the old type.
func ToDeprecatedLogLevel(logLevel LogLevel) *Level {
	var deprecatedLogLevel Level
	switch logLevel {
	case LevelDebug:
		deprecatedLogLevel = DebugLevel
	case LevelInfo:
		deprecatedLogLevel = InfoLevel
	case LevelWarn:
		deprecatedLogLevel = WarnLevel
	case LevelError:
		deprecatedLogLevel = ErrorLevel
	}
	return &deprecatedLogLevel
}

// For transforming an old log level to the new type.
func ToLogLevel(deprecatedLogLevel Level) *LogLevel {
	var newLogLevel LogLevel
	switch deprecatedLogLevel {
	case DebugLevel:
		newLogLevel.Set(LevelDebug)
	case InfoLevel:
		newLogLevel.Set(LevelInfo)
	case WarnLevel:
		newLogLevel.Set(LevelWarn)
	case ErrorLevel:
		newLogLevel.Set(LevelError)
	}
	return &newLogLevel
}

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

	Infof(KeyAll, "Setting log keys to: %v", ConsoleLogKey().EnabledLogKeys())
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

// This provides an io.Writer interface around the base.Infof API
type LoggerWriter struct {
	LogKey                LogKey        // The log key to log to, eg, KeyHTTP
	FormattedSerialNumber string        // The request ID
	Request               *http.Request // The request
	QueryValues           url.Values    // A cached copy of the URL query values
}

// Write() method to satisfy the io.Writer interface
func (lw *LoggerWriter) Write(p []byte) (n int, err error) {
	Infof(lw.LogKey, " %s: %s %s %s", lw.FormattedSerialNumber, lw.Request.Method, SanitizeRequestURL(lw.Request, &lw.QueryValues), string(p))
	return len(p), nil
}

// Create a new LoggerWriter
func NewLoggerWriter(logKey LogKey, formattedSerialNumber string, req *http.Request, queryValues url.Values) *LoggerWriter {
	return &LoggerWriter{
		LogKey:                logKey,
		FormattedSerialNumber: formattedSerialNumber,
		Request:               req,
		QueryValues:           queryValues,
	}
}

// Prepend a context ID to each blip logging message.  The contextID uniquely identifies the blip context, and
// is useful for grouping the blip connections in the log output.
func PrependContextID(contextID, format string, params ...interface{}) (newFormat string, newParams []interface{}) {

	// Add a new format placeholder for the context ID, which should appear at the beginning of the logs
	formatWithContextID := `[%s] ` + format

	params = append(params, 0)
	copy(params[1:], params[0:])
	params[0] = contextID

	return formatWithContextID, params

}

// *************************************************************************
//   2018-04-10: New logging below. Above code is to be removed/cleaned up
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
	Infof(KeyAll, "Rotating log files...")

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

	consoleLogger = newConsoleLoggerOrPanic(&ConsoleLoggerConfig{FileLoggerConfig: FileLoggerConfig{Enabled: BoolPtr(true), CollationBufferSize: &initialCollationBufferSize}})
	initExternalLoggers()
}

// PanicfCtx logs the given formatted string and args to the error log level and given log key and then panics.
func PanicfCtx(ctx context.Context, format string, args ...interface{}) {
	logTo(ctx, LevelError, KeyAll, format, args...)
	FlushLogBuffers()
	panic(fmt.Sprintf(format, args...))
}

// FatalfCtx logs the given formatted string and args to the error log level and given log key and then exits.
func FatalfCtx(ctx context.Context, format string, args ...interface{}) {
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

// Panicf logs the given formatted string and args to the error log level and given log key and then panics.
func Panicf(format string, args ...interface{}) {
	logTo(context.TODO(), LevelError, KeyAll, format, args...)
	FlushLogBuffers()
	panic(fmt.Sprintf(format, args...))
}

// Fatalf logs the given formatted string and args to the error log level and given log key and then exits.
func Fatalf(format string, args ...interface{}) {
	logTo(context.TODO(), LevelError, KeyAll, format, args...)
	FlushLogBuffers()
	os.Exit(1)
}

// Errorf logs the given formatted string and args to the error log level and given log key.
func Errorf(format string, args ...interface{}) {
	logTo(context.TODO(), LevelError, KeyAll, format, args...)
}

// Warnf logs the given formatted string and args to the warn log level and given log key.
func Warnf(format string, args ...interface{}) {
	logTo(context.TODO(), LevelWarn, KeyAll, format, args...)
}

// Infof logs the given formatted string and args to the info log level and given log key.
func Infof(logKey LogKey, format string, args ...interface{}) {
	logTo(context.TODO(), LevelInfo, logKey, format, args...)
}

// Debugf logs the given formatted string and args to the debug log level with an optional log key.
func Debugf(logKey LogKey, format string, args ...interface{}) {
	logTo(context.TODO(), LevelDebug, logKey, format, args...)
}

// Tracef logs the given formatted string and args to the trace log level with an optional log key.
func Tracef(logKey LogKey, format string, args ...interface{}) {
	logTo(context.TODO(), LevelTrace, logKey, format, args...)
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
		SyncGatewayStats.GlobalStats.ResourceUtilization.ErrorCount.Add(1)
	} else if logLevel == LevelWarn {
		SyncGatewayStats.GlobalStats.ResourceUtilization.WarnCount.Add(1)
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

	// Warn and error logs also append caller name/line numbers.
	if logLevel <= LevelWarn && logLevel > LevelNone {
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

// LogSyncGatewayVersion will print the startup indicator and version number to ALL log outputs.
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
		if logCtx, ok := ctx.Value(LogContextKey{}).(LogContext); ok {
			format = logCtx.addContext(format)
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

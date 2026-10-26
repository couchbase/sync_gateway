//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
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
func UpdateLogKeys(ctx context.Context, keys map[string]bool, replace bool) {
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

	InfofCtx(ctx, KeyAll, "Setting log keys to: %v", ConsoleLogKey().EnabledLogKeys())
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
	consoleLogger                                                              atomic.Pointer[ConsoleLogger]
	traceLogger, debugLogger, infoLogger, warnLogger, errorLogger, statsLogger atomic.Pointer[FileLogger]

	auditLogger atomic.Pointer[AuditLogger]

	// envColorCapable evaluated only once to prevent unnecessary
	// overhead of checking os.Getenv on each colorEnabled() invocation
	envColorCapable = runtime.GOOS != "windows" && os.Getenv("TERM") != "dumb"
)

// RotateLogfiles rotates all active log files.
func RotateLogfiles(ctx context.Context) {
	InfofCtx(ctx, KeyAll, "Rotating log files...")

	for _, logger := range getFileLoggers() {
		err := logger.Rotate()
		if err != nil {
			WarnfCtx(ctx, "Error rotating %v: %v", logger, err)
		}
	}
}

func init() {
	initializeLoggers(context.Background())
}

// initializeLoggers should be called once per program in init. This is also called to reset logging in a test context.
func initializeLoggers(ctx context.Context) {
	nilAllNonConsoleLoggers()
	// We'll initilise a default consoleLogger so we can still log stuff before/during parsing logging configs.
	// This maintains consistent formatting (timestamps, levels, etc) in the output,
	// and allows a single set of logging functions to be used, rather than fmt.Printf()

	// initialCollationBufferSize is set to zero for disabling log collation before
	// initializing a logging config, and when running under a test scenario.
	initialCollationBufferSize := 0

	consoleLogger.Store(mustInitConsoleLogger(context.Background(), &ConsoleLoggerConfig{LogKeys: []string{logKeyNames[KeyHTTP]}, FileLoggerConfig: FileLoggerConfig{Enabled: new(true), CollationBufferSize: &initialCollationBufferSize}}))
	updateExternalLoggers()
}

type logFn func(ctx context.Context, format string, args ...any)

// PanicfCtx logs the given formatted string and args to the error log level and given log key and then panics.
func PanicfCtx(ctx context.Context, format string, args ...any) {
	// ensure the log message always reaches console
	ConsolefCtx(ctx, LevelError, KeyAll, format, args...)
	FlushLogBuffers()
	panic(fmt.Sprintf(format, args...))
}

// FatalfCtx logs the given formatted string and args to the error log level and given log key and then exits.
func FatalfCtx(ctx context.Context, format string, args ...any) {
	// no-op call to enable golang.org/x/tools/go/analysis/passes/printf checking in go vet
	if false {
		_ = fmt.Sprintf(format, args...)
	}
	// ensure the log message always reaches console
	ConsolefCtx(ctx, LevelError, KeyAll, format, args...)
	FlushLogBuffers()
	os.Exit(1)
}

// ErrorfCtx logs the given formatted string and args to the error log level and given log key.
func ErrorfCtx(ctx context.Context, format string, args ...any) {
	// no-op call to enable golang.org/x/tools/go/analysis/passes/printf checking in go vet
	if false {
		_ = fmt.Sprintf(format, args...)
	}
	logTo(ctx, LevelError, KeyAll, format, args...)
}

// WarnfCtx logs the given formatted string and args to the warn log level and given log key.
func WarnfCtx(ctx context.Context, format string, args ...any) {
	// no-op call to enable golang.org/x/tools/go/analysis/passes/printf checking in go vet
	if false {
		_ = fmt.Sprintf(format, args...)
	}
	logTo(ctx, LevelWarn, KeyAll, format, args...)
}

// InfofCtx logs the given formatted string and args to the info log level and given log key.
func InfofCtx(ctx context.Context, logKey LogKey, format string, args ...any) {
	// no-op call to enable golang.org/x/tools/go/analysis/passes/printf checking in go vet
	if false {
		_ = fmt.Sprintf(format, args...)
	}
	logTo(ctx, LevelInfo, logKey, format, args...)
}

// DebugfCtx logs the given formatted string and args to the debug log level with an optional log key.
func DebugfCtx(ctx context.Context, logKey LogKey, format string, args ...any) {
	logTo(ctx, LevelDebug, logKey, format, args...)
}

// TracefCtx logs the given formatted string and args to the trace log level with an optional log key.
func TracefCtx(ctx context.Context, logKey LogKey, format string, args ...any) {
	logTo(ctx, LevelTrace, logKey, format, args...)
}

// LogLevelCtx allows logging where the level can be set via parameter.
func LogLevelCtx(ctx context.Context, logLevel LogLevel, logKey LogKey, format string, args ...any) {
	logTo(ctx, logLevel, logKey, format, args...)
}

// RecordStats writes the given stats JSON content to a stats log file, if enabled.
// The content passed in is expected to be a JSON dictionary.
func RecordStats(statsJson string) {
	// if statsLogger is nil, logf will no-op
	statsLogger.Load().log(statsJson)
}

// logTo is the "core" logging function. All other logging functions (like Debugf(), WarnfCtx(), etc.) end up here.
// The function will fan out the log to all of the various outputs for them to decide if they should log it or not.
func logTo(ctx context.Context, logLevel LogLevel, logKey LogKey, format string, args ...any) {
	// no-op call to enable golang.org/x/tools/go/analysis/passes/printf checking in go vet
	if false {
		_ = fmt.Sprintf(format, args...)
	}
	// Defensive bounds-check for log level. All callers of this function should be within this range.
	if logLevel < LevelNone || logLevel >= levelCount {
		return
	}

	if logLevel == LevelError {
		SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.Add(1)
	} else if logLevel == LevelWarn {
		SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Add(1)
	}

	shouldLogConsole := consoleLogger.Load().shouldLog(ctx, logLevel, logKey)
	shouldLogError := errorLogger.Load().shouldLog(logLevel)
	shouldLogWarn := warnLogger.Load().shouldLog(logLevel)
	shouldLogInfo := infoLogger.Load().shouldLog(logLevel)
	shouldLogDebug := debugLogger.Load().shouldLog(logLevel)
	shouldLogTrace := traceLogger.Load().shouldLog(logLevel)

	// exit before string formatting if no loggers need to log
	if !(shouldLogConsole || shouldLogError || shouldLogWarn || shouldLogInfo || shouldLogDebug || shouldLogTrace) {
		return
	}

	// Prepend timestamp, level, log key.
	format = addPrefixes(format, ctx, logLevel, logKey)

	// Error, warn and trace logs also append caller name/line numbers.
	if logLevel <= LevelWarn || logLevel == LevelTrace {
		stackDepth := 2
		if logKey == KeyWalrus {
			stackDepth++ // walrus logs go through another layer of fn call
		}
		format += " -- " + GetCallersName(stackDepth, true)
	}

	// Perform log redaction, if necessary.
	args = redact(args)

	// If either global console or db console wants to log, allow it
	if shouldLogConsole {
		consoleLogger.Load().logf(color(format, logLevel), args...)
	}
	if shouldLogError {
		errorLogger.Load().logf(format, args...)
	}
	if shouldLogWarn {
		warnLogger.Load().logf(format, args...)
	}
	if shouldLogInfo {
		infoLogger.Load().logf(format, args...)
	}
	if shouldLogDebug {
		debugLogger.Load().logf(format, args...)
	}
	if shouldLogTrace {
		traceLogger.Load().logf(format, args...)
	}
}

var consoleFOutput io.Writer = os.Stderr

// ConsolefCtx logs the given formatted string and args to the given log level and log key,
// as well as making sure the message is *always* logged to stdout.
func ConsolefCtx(ctx context.Context, logLevel LogLevel, logKey LogKey, format string, args ...any) {
	logTo(ctx, logLevel, logKey, format, args...)

	logger := consoleLogger.Load()
	// If the above logTo didn't already log to stderr, do it directly here
	if !logger.isStderr || !logger.shouldLog(ctx, logLevel, logKey) {
		format = color(addPrefixes(format, ctx, logLevel, logKey), logLevel)
		_, _ = fmt.Fprintf(consoleFOutput, format+"\n", args...)
	}
}

// LogSyncGatewayVersion will print the '==== name/version ====' startup indicator to ALL log outputs.
func LogSyncGatewayVersion(ctx context.Context) {
	msg := fmt.Sprintf("==== %s ====", LongVersionString)

	// Log the startup indicator to the stderr.
	ConsolefCtx(ctx, LevelNone, KeyNone, "%s", msg)

	// Log the startup indicator to ALL log files too.
	msg = addPrefixes(msg, ctx, LevelNone, KeyNone)
	errorLogger.Load().conditionalPrint(msg)
	warnLogger.Load().conditionalPrint(msg)
	infoLogger.Load().conditionalPrint(msg)
	debugLogger.Load().conditionalPrint(msg)
	traceLogger.Load().conditionalPrint(msg)
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
		var ctxVal any
		for _, k := range allLogContextKeys {
			if ctxVal = ctx.Value(k); ctxVal != nil {
				if logCtx, ok := ctxVal.(ContextAdder); ok {
					format = logCtx.addContext(format)
				}
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
	return consoleLogger.Load().ColorEnabled && envColorCapable
}

// ConsoleLogLevel returns the console log level.
func ConsoleLogLevel() *LogLevel {
	return consoleLogger.Load().LogLevel
}

// ConsoleLogKey returns the console log key.
func ConsoleLogKey() *LogKeyMask {
	return consoleLogger.Load().LogKeyMask
}

// LogInfoEnabled returns true if either the console should log at info level,
// or if the infoLogger is enabled.
func LogInfoEnabled(ctx context.Context, logKey LogKey) bool {
	return consoleLogger.Load().shouldLog(ctx, LevelInfo, logKey) || infoLogger.Load().shouldLog(LevelInfo)
}

// LogDebugEnabled returns true if either the console should log at debug level,
// or if the debugLogger is enabled.
func LogDebugEnabled(ctx context.Context, logKey LogKey) bool {
	return consoleLogger.Load().shouldLog(ctx, LevelDebug, logKey) || debugLogger.Load().shouldLog(LevelDebug)
}

// LogTraceEnabled returns true if either the console should log at trace level,
// or if the traceLogger is enabled.
func LogTraceEnabled(ctx context.Context, logKey LogKey) bool {
	return consoleLogger.Load().shouldLog(ctx, LevelTrace, logKey) || traceLogger.Load().shouldLog(LevelTrace)
}

func LogLevelEnabled(ctx context.Context, level LogLevel, logKey LogKey) bool {
	switch level {
	case LevelError:
		return consoleLogger.Load().shouldLog(ctx, level, logKey) || errorLogger.Load().shouldLog(level)
	case LevelWarn:
		return consoleLogger.Load().shouldLog(ctx, level, logKey) || warnLogger.Load().shouldLog(level)
	case LevelInfo:
		return LogInfoEnabled(ctx, logKey)
	case LevelDebug:
		return LogDebugEnabled(ctx, logKey)
	case LevelTrace:
		return LogTraceEnabled(ctx, logKey)
	case LevelNone:
		return false
	}
	return true
}

// AssertLogContains asserts that the logs produced by function f contain string s.
func AssertLogContains(t *testing.T, s string, f func()) {
	t.Helper()
	assert.Contains(t, consoleLogOutput(f), s)
}

// AssertLogNotContains asserts that the logs produced by function f do not contain string s.
func AssertLogNotContains(t *testing.T, s string, f func()) {
	t.Helper()
	assert.NotContains(t, consoleLogOutput(f), s)
}

// AssertWarningCount asserts that function f logs the expected number of warnings containing logContextField, such as
// "t:"+t.Name() or "db:"+dbName. Unlike the global WarnCount stat, this ignores warnings logged by other tests.
func AssertWarningCount(t testing.TB, logContextField string, expected int, f func()) {
	t.Helper()
	assertLogLevelCount(t, LevelWarn, logContextField, expected, f)
}

// AssertErrorCount asserts that function f logs the expected number of errors containing logContextField, such as
// "t:"+t.Name() or "db:"+dbName. Unlike the global ErrorCount stat, this ignores errors logged by other tests.
func AssertErrorCount(t testing.TB, logContextField string, expected int, f func()) {
	t.Helper()
	assertLogLevelCount(t, LevelError, logContextField, expected, f)
}

// assertLogLevelCount counts console lines, so it does not see lines hidden by a database's own console logging config.
func assertLogLevelCount(t testing.TB, logLevel LogLevel, logContextField string, expected int, f func()) {
	t.Helper()
	// a disabled console logger would make any expected count of 0 pass
	logger := consoleLogger.Load()
	require.Truef(t, shouldLog(logger.LogLevel, logger.LogKeyMask, logLevel, KeyAll), "console logging must be enabled at %s to count log lines", logLevel)
	levelPrefix := "[" + logLevel.StringShort() + "] "
	// trailing space stops "t:TestFoo" matching subtest "t:TestFoo/bar"
	fieldPrefix := logContextField + " "
	var lines []string
	for line := range strings.Lines(consoleLogOutput(f)) {
		// match the level only after the timestamp, so message text such as "[ERR] " is not counted
		_, afterTimestamp, _ := strings.Cut(line, " ")
		if strings.HasPrefix(afterTimestamp, levelPrefix) && strings.Contains(line, fieldPrefix) {
			lines = append(lines, line)
		}
	}
	assert.Len(t, lines, expected)
}

// consoleLogOutput returns the console logs produced by function f. If f replaces the console logger, lines written
// to the new logger are not captured.
func consoleLogOutput(f func()) string {
	logger := consoleLogger.Load().logger
	b := consoleLogCapture.start(logger)
	func() {
		// stop in a defer so that a require failure in f does not leave the capture running
		defer consoleLogCapture.stop(logger, b)
		f()
		FlushLogBuffers()
	}()
	return b.String()
}

var consoleLogCapture = &logCapture{buffers: make(map[*bytes.Buffer]struct{})}

// logCapture copies console log output to every active consoleLogOutput buffer, so that nested captures and
// overlapping captures from parallel tests each see every line.
type logCapture struct {
	startStopLock sync.Mutex // serializes start and stop, never held by Write to avoid lock inversion with log.Logger
	bufferLock    sync.Mutex
	buffers       map[*bytes.Buffer]struct{}
	output        io.Writer // logger output before the first active capture
}

func (c *logCapture) Write(p []byte) (int, error) {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	for b := range c.buffers {
		_, _ = b.Write(p)
	}
	return c.output.Write(p)
}

// start returns a buffer that receives all output from logger until stop is called.
func (c *logCapture) start(logger *log.Logger) *bytes.Buffer {
	c.startStopLock.Lock()
	defer c.startStopLock.Unlock()
	b := &bytes.Buffer{}
	if c.addBuffer(b, logger.Writer()) {
		logger.SetOutput(c)
	}
	return b
}

// stop removes b from the capture, and restores the original logger output when no captures remain.
func (c *logCapture) stop(logger *log.Logger, b *bytes.Buffer) {
	c.startStopLock.Lock()
	defer c.startStopLock.Unlock()
	if output := c.removeBuffer(b); output != nil {
		logger.SetOutput(output)
	}
}

// addBuffer adds b, and returns true if it is the first active capture, in which case output is stored for restore.
func (c *logCapture) addBuffer(b *bytes.Buffer, output io.Writer) (first bool) {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	first = len(c.buffers) == 0
	if first {
		c.output = output
	}
	c.buffers[b] = struct{}{}
	return first
}

// removeBuffer removes b, and returns the original output if no captures remain.
func (c *logCapture) removeBuffer(b *bytes.Buffer) io.Writer {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	delete(c.buffers, b)
	if len(c.buffers) > 0 {
		return nil
	}
	return c.output
}

// AuditLogContents returns that the audit logs produced by function f.
func AuditLogContents(t testing.TB, f func(t testing.TB)) []byte {
	logger := auditLogger.Load()
	prevOutput := logger.logger.Writer()
	b := &bytes.Buffer{}
	func() {
		logger.logger.SetOutput(io.MultiWriter(b, prevOutput))
		// restore in a defer so that a require failure in f does not leave the output redirected
		defer logger.logger.SetOutput(prevOutput)
		f(t)
		FlushLogBuffers()
		logger.FlushBufferToLog()
	}()
	return b.Bytes()
}

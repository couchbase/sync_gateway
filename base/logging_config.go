/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
)

const (
	errorMinAge = 180
	warnMinAge  = 90
	infoMinAge  = 3
	statsMinage = 3
	debugMinAge = 1
	traceMinAge = 1

	// defaultConsoleLoggerCollateBufferSize is the number of console logs we'll
	// buffer and collate, before flushing the buffer to the output.
	defaultConsoleLoggerCollateBufferSize = 10
	defaultFileLoggerCollateBufferSize    = 1000
	// defaultConsoleLoggerCollateFlushTimeout is the amount of time to wait before
	// we flush to the output if we don't fill the buffer.
	consoleLoggerCollateFlushTimeout = 1 * time.Millisecond
	fileLoggerCollateFlushTimeout    = 10 * time.Millisecond
	// loggerCollateFlushDelay is the duration to wait to allow the log collation buffers to be flushed to outputs.
	loggerCollateFlushDelay = 1 * time.Second
)

// ErrUnsetLogFilePath is returned when no log_file_path, or --defaultLogFilePath fallback can be used.
var ErrUnsetLogFilePath = errors.New("No log_file_path property specified in config, and --defaultLogFilePath command line flag was not set. Log files required for product support are not being generated.")

type LegacyLoggingConfig struct {
	LogFilePath    string              `json:"log_file_path,omitempty"`   // Absolute or relative path on the filesystem to the log file directory. A relative path is from the directory that contains the Sync Gateway executable file.
	RedactionLevel RedactionLevel      `json:"redaction_level,omitempty"` // Redaction level to apply to log output.
	Console        ConsoleLoggerConfig `json:"console,omitempty"`         // Console output
	Error          FileLoggerConfig    `json:"error,omitempty"`           // Error log file output
	Warn           FileLoggerConfig    `json:"warn,omitempty"`            // Warn log file output
	Info           FileLoggerConfig    `json:"info,omitempty"`            // Info log file output
	Debug          FileLoggerConfig    `json:"debug,omitempty"`           // Debug log file output
	Trace          FileLoggerConfig    `json:"trace,omitempty"`           // Trace log file output
	Stats          FileLoggerConfig    `json:"stats,omitempty"`           // Stats log file output
}

func InitLogging(logFilePath string,
	console *ConsoleLoggerConfig,
	error, warn, info, debug, trace, stats *FileLoggerConfig) (err error) {

	consoleLogger, err = NewConsoleLogger(true, console)
	if err != nil {
		return err
	}

	// If there's nowhere to specified put log files, we'll log an error, but continue anyway.
	if logFilePath == "" {
		Consolef(LevelInfo, KeyNone, "Logging: Files disabled")
		// Explicitly log this error to console
		Consolef(LevelError, KeyNone, ErrUnsetLogFilePath.Error())

		// nil out other loggers
		errorLogger = nil
		warnLogger = nil
		infoLogger = nil
		debugLogger = nil
		traceLogger = nil
		statsLogger = nil

		return nil
	}

	err = validateLogFilePath(logFilePath)
	if err != nil {
		return err
	} else {
		Consolef(LevelInfo, KeyNone, "Logging: Files to %v", logFilePath)
	}

	errorLogger, err = NewFileLogger(error, LevelError, LevelError.String(), logFilePath, errorMinAge, &errorLogger.buffer)
	if err != nil {
		return err
	}

	warnLogger, err = NewFileLogger(warn, LevelWarn, LevelWarn.String(), logFilePath, warnMinAge, &warnLogger.buffer)
	if err != nil {
		return err
	}

	infoLogger, err = NewFileLogger(info, LevelInfo, LevelInfo.String(), logFilePath, infoMinAge, &infoLogger.buffer)
	if err != nil {
		return err
	}

	debugLogger, err = NewFileLogger(debug, LevelDebug, LevelDebug.String(), logFilePath, debugMinAge, &debugLogger.buffer)
	if err != nil {
		return err
	}

	traceLogger, err = NewFileLogger(trace, LevelTrace, LevelTrace.String(), logFilePath, traceMinAge, &traceLogger.buffer)
	if err != nil {
		return err
	}

	// Since there is no level checking in the stats logging, use LevelNone for the level.
	statsLogger, err = NewFileLogger(stats, LevelNone, "stats", logFilePath, statsMinage, &statsLogger.buffer)
	if err != nil {
		return err
	}

	// Initialize external loggers too
	initExternalLoggers()

	return nil
}

// NewMemoryLogger will log to a buffer, which can then be flushed out elsewhere later.
func NewMemoryLogger(level LogLevel) *FileLogger {
	logger := &FileLogger{
		Enabled: AtomicBool{1},
		level:   level,
		name:    level.String(),
	}
	logger.output = &logger.buffer
	logger.logger = log.New(&logger.buffer, "", 0)

	return logger
}

// InitializeMemoryLoggers will set the global loggers to a in-memory logging buffer, to be flushed to configured outputs at a later time.
func InitializeMemoryLoggers() {
	errorLogger = NewMemoryLogger(LevelError)
	warnLogger = NewMemoryLogger(LevelWarn)
	infoLogger = NewMemoryLogger(LevelInfo)
	debugLogger = NewMemoryLogger(LevelDebug)
	traceLogger = NewMemoryLogger(LevelTrace)
	statsLogger = NewMemoryLogger(LevelNone)
}

func FlushLoggerBuffers() {
	if errorLogger != nil {
		errorLogger.FlushBufferToLog()
	}
	if warnLogger != nil {
		warnLogger.FlushBufferToLog()
	}
	if infoLogger != nil {
		infoLogger.FlushBufferToLog()
	}
	if debugLogger != nil {
		debugLogger.FlushBufferToLog()
	}
	if traceLogger != nil {
		traceLogger.FlushBufferToLog()
	}
	if statsLogger != nil {
		statsLogger.FlushBufferToLog()
	}
}

func EnableErrorLogger(enabled bool) {
	if errorLogger != nil {
		errorLogger.Enabled.Set(enabled)
	}
}

func EnableWarnLogger(enabled bool) {
	if warnLogger != nil {
		warnLogger.Enabled.Set(enabled)
	}
}

func EnableInfoLogger(enabled bool) {
	if infoLogger != nil {
		infoLogger.Enabled.Set(enabled)
	}
}

func EnableDebugLogger(enabled bool) {
	if debugLogger != nil {
		debugLogger.Enabled.Set(enabled)
	}
}

func EnableTraceLogger(enabled bool) {
	if traceLogger != nil {
		traceLogger.Enabled.Set(enabled)
	}
}

func EnableStatsLogger(enabled bool) {
	if statsLogger != nil {
		statsLogger.Enabled.Set(enabled)
	}
}

// === Used by tests only ===
func ErrorLoggerIsEnabled() bool {
	return errorLogger.Enabled.IsTrue()
}

func WarnLoggerIsEnabled() bool {
	return warnLogger.Enabled.IsTrue()
}

func InfoLoggerIsEnabled() bool {
	return infoLogger.Enabled.IsTrue()
}

func DebugLoggerIsEnabled() bool {
	return debugLogger.Enabled.IsTrue()
}

func TraceLoggerIsEnabled() bool {
	return traceLogger.Enabled.IsTrue()
}

func StatsLoggerIsEnabled() bool {
	return statsLogger.Enabled.IsTrue()
}

type LoggingConfig struct {
	LogFilePath    string               `json:"log_file_path,omitempty"   help:"Absolute or relative path on the filesystem to the log file directory. A relative path is from the directory that contains the Sync Gateway executable file"`
	RedactionLevel RedactionLevel       `json:"redaction_level,omitempty" help:"Redaction level to apply to log output"`
	Console        *ConsoleLoggerConfig `json:"console,omitempty"`
	Error          *FileLoggerConfig    `json:"error,omitempty"`
	Warn           *FileLoggerConfig    `json:"warn,omitempty"`
	Info           *FileLoggerConfig    `json:"info,omitempty"`
	Debug          *FileLoggerConfig    `json:"debug,omitempty"`
	Trace          *FileLoggerConfig    `json:"trace,omitempty"`
	Stats          *FileLoggerConfig    `json:"stats,omitempty"`
}

func BuildLoggingConfigFromLoggers(redactionLevel RedactionLevel, LogFilePath string) *LoggingConfig {
	config := LoggingConfig{
		RedactionLevel: redactionLevel,
		LogFilePath:    LogFilePath,
	}

	config.Console = consoleLogger.getConsoleLoggerConfig()
	config.Error = errorLogger.getFileLoggerConfig()
	config.Warn = warnLogger.getFileLoggerConfig()
	config.Info = infoLogger.getFileLoggerConfig()
	config.Debug = debugLogger.getFileLoggerConfig()
	config.Trace = traceLogger.getFileLoggerConfig()
	config.Stats = statsLogger.getFileLoggerConfig()

	return &config
}

// validateLogFilePath ensures the given path is created and is a directory.
func validateLogFilePath(logFilePath string) error {
	// Make full directory structure if it doesn't already exist
	err := os.MkdirAll(logFilePath, 0700)
	if err != nil {
		return errors.Wrap(err, ErrInvalidLogFilePath.Error())
	}

	// Ensure LogFilePath is a directory. Lumberjack will check file permissions when it opens/creates the logfile.
	if f, err := os.Stat(logFilePath); err != nil {
		return errors.Wrap(err, ErrInvalidLogFilePath.Error())
	} else if !f.IsDir() {
		return errors.Wrap(ErrInvalidLogFilePath, "not a directory")
	}

	// Make temporary empty file to check if the log file path is writable
	writeCheckFilePath := filepath.Join(logFilePath, ".SG_write_check")
	err = os.WriteFile(writeCheckFilePath, nil, 0666)
	if err != nil {
		return errors.Wrap(err, ErrUnwritableLogFilePath.Error())
	}
	// best effort cleanup, but if we don't manage to remove it, WriteFile will overwrite on the next startup and try to remove again
	_ = os.Remove(writeCheckFilePath)

	return nil
}

// validateLogFileOutput ensures the given file has permission to be written to.
func validateLogFileOutput(logFileOutput string) error {
	if logFileOutput == "" {
		return fmt.Errorf("empty log file output")
	}

	// Validate containing directory
	logFileOutputDirectory := filepath.Dir(logFileOutput)
	err := validateLogFilePath(logFileOutputDirectory)
	if err != nil {
		return err
	}

	// Validate given file is writeable
	file, err := os.OpenFile(logFileOutput, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		if os.IsPermission(err) {
			return errors.Wrap(err, "invalid file output")
		}
		return err
	}

	return file.Close()
}

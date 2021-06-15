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

type LoggingConfig struct {
	LogFilePath          string              `json:"log_file_path,omitempty"`   // Absolute or relative path on the filesystem to the log file directory. A relative path is from the directory that contains the Sync Gateway executable file.
	RedactionLevel       RedactionLevel      `json:"redaction_level,omitempty"` // Redaction level to apply to log output.
	Console              ConsoleLoggerConfig `json:"console,omitempty"`         // Console output
	Error                FileLoggerConfig    `json:"error,omitempty"`           // Error log file output
	Warn                 FileLoggerConfig    `json:"warn,omitempty"`            // Warn log file output
	Info                 FileLoggerConfig    `json:"info,omitempty"`            // Info log file output
	Debug                FileLoggerConfig    `json:"debug,omitempty"`           // Debug log file output
	Trace                FileLoggerConfig    `json:"trace,omitempty"`           // Trace log file output
	Stats                FileLoggerConfig    `json:"stats,omitempty"`           // Stats log file output
	DeprecatedDefaultLog *LogAppenderConfig  `json:"default,omitempty"`         // Deprecated "default" logging option.
}

func InitLogging(defaultLogFilePath, logFilePath string,
	console *ConsoleLoggerConfig,
	error, warn, info, debug, trace, stats *FileLoggerConfig) (err error) {

	consoleLogger, err = NewConsoleLogger(true, console)
	if err != nil {
		return err
	}

	// If there's nowhere to specified put log files, we'll log an error, but continue anyway.
	if !hasLogFilePath(&logFilePath, defaultLogFilePath) {
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

	err = validateLogFilePath(&logFilePath, defaultLogFilePath)
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

func NewMemoryLogger(level LogLevel) *FileLogger {
	logger := &FileLogger{
		Enabled: true,
		level:   level,
		name:    level.String(),
	}
	logger.output = &logger.buffer
	logger.logger = log.New(&logger.buffer, "", 0)

	return logger
}

func InitializeLoggers() {
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

// validateLogFilePath ensures the given path is created and is a directory.
func validateLogFilePath(logFilePath *string, defaultLogFilePath string) error {
	if logFilePath == nil || *logFilePath == "" {
		*logFilePath = defaultLogFilePath
	}

	// Make full directory structure if it doesn't already exist
	err := os.MkdirAll(*logFilePath, 0700)
	if err != nil {
		return errors.Wrap(err, ErrInvalidLogFilePath.Error())
	}

	// Ensure LogFilePath is a directory. Lumberjack will check file permissions when it opens/creates the logfile.
	if f, err := os.Stat(*logFilePath); err != nil {
		return errors.Wrap(err, ErrInvalidLogFilePath.Error())
	} else if !f.IsDir() {
		return errors.Wrap(ErrInvalidLogFilePath, "not a directory")
	}

	return nil
}

// validateLogFileOutput ensures the given file has permission to be written to.
func validateLogFileOutput(logFileOutput string) error {
	if logFileOutput == "" {
		return fmt.Errorf("empty log file output")
	}

	// Validate containing directory
	logFileOutputDirectory := filepath.Dir(logFileOutput)
	err := validateLogFilePath(&logFileOutputDirectory, "")
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

// hasLogFilePath returns true if there's either a logFilePath set, or we can fall back to a defaultLogFilePath.
func hasLogFilePath(logFilePath *string, defaultLogFilePath string) bool {
	return (logFilePath != nil && *logFilePath != "") || defaultLogFilePath != ""
}

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
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const (
	errorMinAge = 180
	warnMinAge  = 90
	infoMinAge  = 3
	debugMinAge = 1
	traceMinAge = 1
	statsMinage = 3
	auditMinage = 3

	// defaultConsoleLoggerCollateBufferSize is the number of console logs we'll
	// buffer and collate, before flushing the buffer to the output.
	defaultConsoleLoggerCollateBufferSize = 10
	defaultFileLoggerCollateBufferSize    = 1000
	// defaultConsoleLoggerCollateFlushTimeout is the amount of time to wait before
	// we flush to the output if we don't fill the buffer.
	consoleLoggerCollateFlushTimeout = 1 * time.Millisecond
	fileLoggerCollateFlushTimeout    = 10 * time.Millisecond

	rotatedLogDeletionInterval = time.Hour // not configurable
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

func InitLogging(ctx context.Context, logFilePath string,
	console *ConsoleLoggerConfig,
	error, warn, info, debug, trace, stats *FileLoggerConfig,
	audit *AuditLoggerConfig, auditLogGlobalFields map[string]any) (err error) {

	rawConsoleLogger, err := NewConsoleLogger(ctx, true, console)
	if err != nil {
		return err
	}
	consoleLogger.Store(rawConsoleLogger)

	// If there's nowhere to specified put log files, we'll log an error, but continue anyway.
	if logFilePath == "" {
		ConsolefCtx(ctx, LevelInfo, KeyNone, "Logging: Files disabled")
		// Explicitly log this error to console
		ConsolefCtx(ctx, LevelError, KeyNone, ErrUnsetLogFilePath.Error())
		nilAllNonConsoleLoggers()
		return nil
	}

	err = validateLogFilePath(logFilePath)
	if err != nil {
		return err
	}

	ConsolefCtx(ctx, LevelInfo, KeyNone, "Logging: Files to %v", logFilePath)

	auditLogFilePath := logFilePath
	if audit != nil && audit.AuditLogFilePath != nil && BoolDefault(audit.Enabled, false) {
		auditLogFilePath = *audit.AuditLogFilePath
		err = validateLogFilePath(auditLogFilePath)
		if err != nil {
			return fmt.Errorf("error validating audit log file path: %w", err)
		}
		ConsolefCtx(ctx, LevelInfo, KeyNone, "Logging: Audit to %v", auditLogFilePath)
	}

	rawErrorlogger, err := NewFileLogger(ctx, error, LevelError, LevelError.String(), logFilePath, errorMinAge, &errorLogger.Load().buffer)
	if err != nil {
		return err
	}
	errorLogger.Store(rawErrorlogger)

	rawWarnLogger, err := NewFileLogger(ctx, warn, LevelWarn, LevelWarn.String(), logFilePath, warnMinAge, &warnLogger.Load().buffer)
	if err != nil {
		return err
	}
	warnLogger.Store(rawWarnLogger)

	rawInfoLogger, err := NewFileLogger(ctx, info, LevelInfo, LevelInfo.String(), logFilePath, infoMinAge, &infoLogger.Load().buffer)
	if err != nil {
		return err
	}
	infoLogger.Store(rawInfoLogger)

	rawDebugLogger, err := NewFileLogger(ctx, debug, LevelDebug, LevelDebug.String(), logFilePath, debugMinAge, &debugLogger.Load().buffer)
	if err != nil {
		return err
	}
	debugLogger.Store(rawDebugLogger)

	rawTraceLogger, err := NewFileLogger(ctx, trace, LevelTrace, LevelTrace.String(), logFilePath, traceMinAge, &traceLogger.Load().buffer)
	if err != nil {
		return err
	}
	traceLogger.Store(rawTraceLogger)

	// Since there is no level checking in the stats logging, use LevelNone for the level.
	rawStatsLogger, err := NewFileLogger(ctx, stats, LevelNone, "stats", logFilePath, statsMinage, &statsLogger.Load().buffer)
	if err != nil {
		return err
	}
	statsLogger.Store(rawStatsLogger)

	var auditLoggerBuffer *strings.Builder
	prevAuditLogger := auditLogger.Load()
	if prevAuditLogger != nil {
		auditLoggerBuffer = &prevAuditLogger.buffer
	}
	rawAuditLogger, err := NewAuditLogger(ctx, audit, auditLogFilePath, auditMinage, auditLoggerBuffer, auditLogGlobalFields)
	if err != nil {
		return err
	}
	auditLogger.Store(rawAuditLogger)

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
	errorLogger.Store(NewMemoryLogger(LevelError))
	warnLogger.Store(NewMemoryLogger(LevelWarn))
	infoLogger.Store(NewMemoryLogger(LevelInfo))
	debugLogger.Store(NewMemoryLogger(LevelDebug))
	traceLogger.Store(NewMemoryLogger(LevelTrace))
	statsLogger.Store(NewMemoryLogger(LevelNone))
}

func FlushLoggerBuffers() {
	for _, logger := range getFileLoggers() {
		logger.FlushBufferToLog()
	}
}

func EnableErrorLogger(enabled bool) {
	logger := errorLogger.Load()
	if logger == nil {
		return
	}
	logger.Enabled.Set(enabled)
}

func EnableWarnLogger(enabled bool) {
	logger := warnLogger.Load()
	if logger == nil {
		return
	}
	logger.Enabled.Set(enabled)
}

func EnableInfoLogger(enabled bool) {
	logger := infoLogger.Load()
	if logger == nil {
		return
	}
	logger.Enabled.Set(enabled)
}

func EnableDebugLogger(enabled bool) {
	logger := debugLogger.Load()
	if logger == nil {
		return
	}
	logger.Enabled.Set(enabled)
}

func EnableTraceLogger(enabled bool) {
	logger := traceLogger.Load()
	if logger == nil {
		return
	}
	logger.Enabled.Set(enabled)
}

func EnableStatsLogger(enabled bool) {
	logger := statsLogger.Load()
	if logger == nil {
		return
	}
	logger.Enabled.Set(enabled)
}

func EnableAuditLogger(ctx context.Context, enabled bool) {
	logger := auditLogger.Load()
	if logger == nil {
		return
	}
	if !enabled {
		Audit(ctx, AuditIDAuditDisabled, AuditFields{AuditFieldAuditScope: "global"})
	}
	logger.Enabled.Set(enabled)
	if enabled {
		Audit(ctx, AuditIDAuditEnabled, AuditFields{AuditFieldAuditScope: "global"})
	}
}

// === Used by tests only ===
func ErrorLoggerIsEnabled() bool {
	return errorLogger.Load().Enabled.IsTrue()
}

func WarnLoggerIsEnabled() bool {
	return warnLogger.Load().Enabled.IsTrue()
}

func InfoLoggerIsEnabled() bool {
	return infoLogger.Load().Enabled.IsTrue()
}

func DebugLoggerIsEnabled() bool {
	return debugLogger.Load().Enabled.IsTrue()
}

func TraceLoggerIsEnabled() bool {
	return traceLogger.Load().Enabled.IsTrue()
}

func StatsLoggerIsEnabled() bool {
	return statsLogger.Load().Enabled.IsTrue()
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
	Audit          *AuditLoggerConfig   `json:"audit,omitempty"`
}

type AuditLoggerConfig struct {
	FileLoggerConfig
	AuditLogFilePath *string `json:"audit_log_file_path,omitempty"` // If set, overrides the output path for the audit log files
	EnabledEvents    []uint  `json:"enabled_events,omitempty"`
}

func BuildLoggingConfigFromLoggers(originalConfig LoggingConfig) *LoggingConfig {
	config := LoggingConfig{
		RedactionLevel: originalConfig.RedactionLevel,
		LogFilePath:    originalConfig.LogFilePath,
	}

	config.Console = consoleLogger.Load().getConsoleLoggerConfig()
	config.Error = errorLogger.Load().getFileLoggerConfig()
	config.Warn = warnLogger.Load().getFileLoggerConfig()
	config.Info = infoLogger.Load().getFileLoggerConfig()
	config.Debug = debugLogger.Load().getFileLoggerConfig()
	config.Trace = traceLogger.Load().getFileLoggerConfig()
	config.Stats = statsLogger.Load().getFileLoggerConfig()
	config.Audit = auditLogger.Load().getAuditLoggerConfig()

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

// nilAllNonConsoleLoggers will nil out all loggers except the console logger.
func nilAllNonConsoleLoggers() {
	errorLogger.Store(nil)
	warnLogger.Store(nil)
	infoLogger.Store(nil)
	debugLogger.Store(nil)
	traceLogger.Store(nil)
	statsLogger.Store(nil)
	auditLogger.Store(nil)
}

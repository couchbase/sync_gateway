package base

import (
	"fmt"
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

	// defaultConsoleLoggerCollateBufferSize is the number of console logs we'll
	// buffer and collate, before flushing the buffer to the output.
	defaultConsoleLoggerCollateBufferSize = 10
	defaultFileLoggerCollateBufferSize    = 1000
	// loggerCollateFlushTimeout is the amount of time to wait before
	// we flush to the output if we don't fill the buffer.
	loggerCollateFlushTimeout = 10 * time.Millisecond
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
	Stats                FileLoggerConfig    `json:"stats,omitempty"`           // Stats log file output
	DeprecatedDefaultLog *LogAppenderConfig  `json:"default,omitempty"`         // Deprecated "default" logging option.
}

// Init will initialize logging, return any warnings that need to be logged at a later time.
func (c *LoggingConfig) Init(defaultLogFilePath string) (warnings []DeferredLogFn, err error) {
	if c == nil {
		return warnings, errors.New("nil LoggingConfig")
	}

	consoleLogger, warnings, err = NewConsoleLogger(&c.Console)
	if err != nil {
		return warnings, err
	}

	// If there's nowhere to specified put log files, we'll log an error, but continue anyway.
	if !hasLogFilePath(&c.LogFilePath, defaultLogFilePath) {
		warnings = append(warnings, func() {
			Consolef(LevelInfo, KeyNone, "Logging: Files disabled")
			// Explicitly log this error to console
			Consolef(LevelError, KeyNone, ErrUnsetLogFilePath.Error())
		})
		return warnings, nil
	}

	err = validateLogFilePath(&c.LogFilePath, defaultLogFilePath)
	if err != nil {
		return warnings, err
	} else {
		warnings = append(warnings, func() {
			Consolef(LevelInfo, KeyNone, "Logging: Files to %v", c.LogFilePath)
		})
	}

	errorLogger, err = NewFileLogger(c.Error, LevelError, LevelError.String(), c.LogFilePath, errorMinAge)
	if err != nil {
		return warnings, err
	}

	warnLogger, err = NewFileLogger(c.Warn, LevelWarn, LevelWarn.String(), c.LogFilePath, warnMinAge)
	if err != nil {
		return warnings, err
	}

	infoLogger, err = NewFileLogger(c.Info, LevelInfo, LevelInfo.String(), c.LogFilePath, infoMinAge)
	if err != nil {
		return warnings, err
	}

	debugLogger, err = NewFileLogger(c.Debug, LevelDebug, LevelDebug.String(), c.LogFilePath, debugMinAge)
	if err != nil {
		return warnings, err
	}

	// Since there is no level checking in the stats logging, use LevelNone for the level.
	statsLogger, err = NewFileLogger(c.Stats, LevelNone, "stats", c.LogFilePath, statsMinage)
	if err != nil {
		return warnings, err
	}

	// Initialize external loggers too
	initExternalLoggers()

	return warnings, nil
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

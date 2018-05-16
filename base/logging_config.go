package base

import (
	"os"

	"github.com/pkg/errors"
)

const (
	errorMinAge = 180
	warnMinAge  = 90
	infoMinAge  = 3
	debugMinAge = 1
)

// ErrUnsetLogFilePath is returned when no logFilePath, or --defaultLogFilePath fallback can be used.
var ErrUnsetLogFilePath = errors.New("No logFilePath configured, and --defaultLogFilePath flag is not set. Log files required for product support are not being generated.")

type LoggingConfig struct {
	LogFilePath    string              `json:"log_file_path,omitempty"`   // Absolute or relative path on the filesystem to the log file directory. A relative path is from the directory that contains the Sync Gateway executable file.
	RedactionLevel RedactionLevel      `json:"redaction_level,omitempty"` // Redaction level to apply to log output.
	Console        ConsoleLoggerConfig `json:"console,omitempty"`         // Console output
	Error          FileLoggerConfig    `json:"error,omitempty"`           // Error log file output
	Warn           FileLoggerConfig    `json:"warn,omitempty"`            // Warn log file output
	Info           FileLoggerConfig    `json:"info,omitempty"`            // Info log file output
	Debug          FileLoggerConfig    `json:"debug,omitempty"`           // Debug log file output

	DeprecatedDefaultLog *LogAppenderConfig `json:"default,omitempty"` // Deprecated "default" logging option.
}

func (c *LoggingConfig) Init(defaultLogFilePath string) (deferredLogs []DeferredLog, err error) {
	if c == nil {
		return deferredLogs, errors.New("nil LoggingConfig")
	}

	consoleLogger, deferredLogs, err = NewConsoleLogger(&c.Console)
	if err != nil {
		return
	}

	// If there's nowhere to specified put log files, we'll log an error, but we'll continue anyway.
	if !hasLogFilePath(&c.LogFilePath, defaultLogFilePath) {
		return deferredLogs, ErrUnsetLogFilePath
	}

	err = validateLogFilePath(&c.LogFilePath, defaultLogFilePath)
	if err != nil {
		return
	}

	errorLogger, err = NewFileLogger(c.Error, LevelError, c.LogFilePath, errorMinAge)
	if err != nil {
		return
	}

	warnLogger, err = NewFileLogger(c.Warn, LevelWarn, c.LogFilePath, warnMinAge)
	if err != nil {
		return
	}

	infoLogger, err = NewFileLogger(c.Info, LevelInfo, c.LogFilePath, infoMinAge)
	if err != nil {
		return
	}

	debugLogger, err = NewFileLogger(c.Debug, LevelDebug, c.LogFilePath, debugMinAge)
	if err != nil {
		return
	}

	return deferredLogs, nil
}

// validateLogFilePath ensures the given path is created and is a directory.
func validateLogFilePath(logFilePath *string, defaultLogFilePath string) error {
	if logFilePath == nil || *logFilePath == "" {
		*logFilePath = defaultLogFilePath
	}

	err := os.MkdirAll(*logFilePath, 0700)
	if err != nil {
		return errors.Wrap(err, ErrInvalidLogFilePath.Error())
	}

	// Ensure LogFilePath is a directory. Lumberjack will check permissions when it opens the logfile.
	if f, err := os.Stat(*logFilePath); err != nil {
		return errors.Wrap(err, ErrInvalidLogFilePath.Error())
	} else if !f.IsDir() {
		return errors.Wrap(ErrInvalidLogFilePath, "not a directory")
	}

	return nil
}

// hasLogFilePath returns true if there's either a logFilePath set, or we can fall back to a defaultLogFilePath.
func hasLogFilePath(logFilePath *string, defaultLogFilePath string) bool {
	return (logFilePath != nil && *logFilePath != "") || defaultLogFilePath != ""
}

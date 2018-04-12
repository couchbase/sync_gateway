package base

import (
	"github.com/pkg/errors"
)

const (
	errorMinAge = 180
	warnMinAge  = 90
	infoMinAge  = 3
	debugMinAge = 1
)

type LoggingConfig struct {
	LogFilePath    string              `json:",omitempty"` // Absolute or relative path on the filesystem to the log file directory. A relative path is from the directory that contains the Sync Gateway executable file.
	RedactionLevel RedactionLevel      `json:",omitempty"`
	Console        ConsoleLoggerConfig `json:",omitempty"` // Console Output
	Error          FileLoggerConfig    `json:",omitempty"` // Error log file output
	Warn           FileLoggerConfig    `json:",omitempty"` // Warn log file output
	Info           FileLoggerConfig    `json:",omitempty"` // Info log file output
	Debug          FileLoggerConfig    `json:",omitempty"` // Debug log file output

	DeprecatedDefaultLog *LogAppenderConfig `json:"default,omitempty"` // Deprecated "default" logging option.
}

func (c *LoggingConfig) Init() error {
	if c == nil {
		return errors.New("invalid LoggingConfig")
	}

	err := validateLogFilePath(&c.LogFilePath)
	if err != nil {
		return err
	}

	consoleLogger, err = NewConsoleLogger(c.Console)
	if err != nil {
		return err
	}

	errorLogger, err = NewFileLogger(c.Error, LevelError, c.LogFilePath, errorMinAge)
	if err != nil {
		return err
	}

	warnLogger, err = NewFileLogger(c.Warn, LevelWarn, c.LogFilePath, warnMinAge)
	if err != nil {
		return err
	}

	infoLogger, err = NewFileLogger(c.Info, LevelInfo, c.LogFilePath, infoMinAge)
	if err != nil {
		return err
	}

	debugLogger, err = NewFileLogger(c.Debug, LevelDebug, c.LogFilePath, debugMinAge)
	if err != nil {
		return err
	}

	return nil
}

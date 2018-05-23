package base

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

type ConsoleLogger struct {
	LogLevel     *LogLevel
	LogKey       *LogKey
	ColorEnabled bool

	logger *log.Logger
}

type ConsoleLoggerConfig struct {
	LogLevel     *LogLevel `json:"log_level,omitempty"`     // Log Level for the console output
	LogKeys      []string  `json:"log_keys,omitempty"`      // Log Keys for the console output
	ColorEnabled *bool     `json:"color_enabled,omitempty"` // Log with color for the console output

	Output io.Writer `json:"-"` // Logger output. Defaults to os.Stderr. Can be overridden for testing purposes.
}

// NewConsoleLogger returns a new ConsoleLogger from a config.
func NewConsoleLogger(config *ConsoleLoggerConfig) (*ConsoleLogger, []DeferredLogFn, error) {
	// validate and set defaults
	if err := config.init(); err != nil {
		return nil, nil, err
	}

	logKey, deferredLogs := ToLogKey(config.LogKeys)

	return &ConsoleLogger{
		LogLevel:     config.LogLevel,
		LogKey:       &logKey,
		ColorEnabled: *config.ColorEnabled,
		logger:       log.New(config.Output, "", 0),
	}, deferredLogs, nil
}

// shouldLog returns true if the given logLevel and logKey should get logged.
func (l *ConsoleLogger) shouldLog(logLevel LogLevel, logKey LogKey) bool {
	return l != nil &&
		l.logger != nil &&
		l.LogLevel.Enabled(logLevel) &&
		// if logging at KEY_ALL, allow it unless KEY_NONE is set
		((logKey == KeyAll && !l.LogKey.Enabled(KeyNone)) ||
			// Otherwise check the given log key is enabled
			l.LogKey.Enabled(logKey))
}

// init validates and sets any defaults for the given ConsoleLoggerConfig
func (lcc *ConsoleLoggerConfig) init() error {
	if lcc == nil {
		return errors.New("nil LogConsoleConfig")
	}

	// Default to os.Stderr if alternative output is not set
	if lcc.Output == nil {
		lcc.Output = os.Stderr
	}

	// Default log level
	if lcc.LogLevel == nil {
		newLevel := LevelInfo
		lcc.LogLevel = &newLevel
	} else if *lcc.LogLevel < LevelNone || *lcc.LogLevel > levelCount {
		return fmt.Errorf("invalid log level: %v", *lcc.LogLevel)
	}

	// Always enable the HTTP log key
	lcc.LogKeys = append(lcc.LogKeys, logKeyNames[KeyHTTP])

	// If ColorEnabled is not explicitly set, use the value of $SG_COLOR
	if lcc.ColorEnabled == nil {
		// Ignore error parsing this value to treat it as false.
		color, _ := strconv.ParseBool(os.Getenv("SG_COLOR"))
		lcc.ColorEnabled = &color
	}

	return nil
}

func newConsoleLoggerOrPanic(config *ConsoleLoggerConfig) *ConsoleLogger {
	logger, _, err := NewConsoleLogger(config)
	if err != nil {
		panic(err)
	}
	return logger
}

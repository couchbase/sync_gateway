package base

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/natefinch/lumberjack"
)

// ConsoleLogger is a file logger with a default output of stderr, and tunable log level/keys.
type ConsoleLogger struct {
	FileLogger

	LogLevel     *LogLevel
	LogKey       *LogKey
	ColorEnabled bool

	// isStderr is true when the console logger is configured with no FileOutput
	isStderr bool
}

type ConsoleLoggerConfig struct {
	FileLoggerConfig

	LogLevel     *LogLevel `json:"log_level,omitempty"`     // Log Level for the console output
	LogKeys      []string  `json:"log_keys,omitempty"`      // Log Keys for the console output
	ColorEnabled *bool     `json:"color_enabled,omitempty"` // Log with color for the console output

	// FileOutput can be used to override the default stderr output, and write to the file specified instead.
	FileOutput string `json:"file_output,omitempty"`
}

// NewConsoleLogger returns a new ConsoleLogger from a config.
func NewConsoleLogger(config *ConsoleLoggerConfig) (*ConsoleLogger, []DeferredLogFn, error) {
	// validate and set defaults
	if err := config.init(); err != nil {
		return nil, nil, err
	}

	logKey, warnings := ToLogKey(config.LogKeys)
	isStderr := config.FileOutput == ""

	logger := &ConsoleLogger{
		LogLevel:     config.LogLevel,
		LogKey:       &logKey,
		ColorEnabled: *config.ColorEnabled && isStderr,
		FileLogger: FileLogger{
			logger: log.New(config.Output, "", 0),
		},
		isStderr: isStderr,
	}

	// Only create the collateBuffer channel and worker if required.
	if *config.CollationBufferSize > 1 {
		logger.collateBuffer = make(chan string, *config.CollationBufferSize)

		// Start up a single worker to consume messages from the buffer
		go logCollationWorker(logger.collateBuffer, logger.logger, *config.CollationBufferSize)
	}

	return logger, warnings, nil
}

func (l *ConsoleLogger) logf(format string, args ...interface{}) {
	if l.collateBuffer != nil {
		l.collateBuffer <- fmt.Sprintf(format, args...)
	} else {
		l.logger.Printf(format, args...)
	}
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

	if lcc.Rotation.MaxSize == nil {
		// A value of zero disables the log file rotation in Lumberjack.
		zero := 0
		lcc.Rotation.MaxSize = &zero
	} else if *lcc.Rotation.MaxSize < 0 {
		return fmt.Errorf(belowMinValueFmt, "MaxSize", "console", *lcc.Rotation.MaxSize, 0)
	}

	if lcc.Rotation.MaxAge == nil {
		// A value of zero disables the age-based log cleanup in Lumberjack.
		zero := 0
		lcc.Rotation.MaxAge = &zero
	} else if *lcc.Rotation.MaxAge < 0 {
		return fmt.Errorf(belowMinValueFmt, "MaxAge", "console", *lcc.Rotation.MaxAge, 0)
	} else if *lcc.Rotation.MaxAge > maxAgeLimit {
		return fmt.Errorf(aboveMaxValueFmt, "MaxAge", "console", *lcc.Rotation.MaxAge, maxAgeLimit)
	}

	// Default to os.Stderr if alternative output is not set
	if lcc.Output == nil && lcc.FileOutput == "" {
		lcc.Output = os.Stderr
	} else if lcc.FileOutput != "" {
		// Otherwise check permissions on the given output and create a Lumberjack logger
		if err := validateLogFileOutput(lcc.FileOutput); err != nil {
			return err
		}
		lcc.Output = &lumberjack.Logger{
			Filename: filepath.FromSlash(lcc.FileOutput),
			MaxSize:  *lcc.Rotation.MaxSize,
			MaxAge:   *lcc.Rotation.MaxAge,
			Compress: false,
		}
	}

	// Default to false
	if lcc.Enabled == nil || !*lcc.Enabled {
		newLevel := LevelNone
		lcc.LogLevel = &newLevel
		lcc.LogKeys = []string{}
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

	// Default to consoleLoggerCollateBufferSize if a collation buffer size is not set
	if lcc.CollationBufferSize == nil {
		bufferSize := 0
		if *lcc.LogLevel >= LevelInfo {
			bufferSize = defaultConsoleLoggerCollateBufferSize
		}
		lcc.CollationBufferSize = &bufferSize
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

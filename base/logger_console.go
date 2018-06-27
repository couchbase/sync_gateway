package base

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type ConsoleLogger struct {
	LogLevel     *LogLevel
	LogKey       *LogKey
	ColorEnabled bool

	// collateBuffer is used to store log entries to batch up multiple logs.
	collateBuffer chan string
	logger        *log.Logger
}

type ConsoleLoggerConfig struct {
	Enabled      *bool     `json:"enabled,omitempty"`       // Overall console output toggle
	LogLevel     *LogLevel `json:"log_level,omitempty"`     // Log Level for the console output
	LogKeys      []string  `json:"log_keys,omitempty"`      // Log Keys for the console output
	ColorEnabled *bool     `json:"color_enabled,omitempty"` // Log with color for the console output

	CollationBufferSize *int      `json:"collation_buffer_size,omitempty"` // The size of the log collation buffer.
	Output              io.Writer `json:"-"`                               // Logger output. Defaults to os.Stderr. Can be overridden for testing purposes.
}

// NewConsoleLogger returns a new ConsoleLogger from a config.
func NewConsoleLogger(config *ConsoleLoggerConfig) (*ConsoleLogger, []DeferredLogFn, error) {
	// validate and set defaults
	if err := config.init(); err != nil {
		return nil, nil, err
	}

	logKey, warnings := ToLogKey(config.LogKeys)

	logger := &ConsoleLogger{
		LogLevel:     config.LogLevel,
		LogKey:       &logKey,
		ColorEnabled: *config.ColorEnabled,
		logger:       log.New(config.Output, "", 0),
	}

	// Only create the collateBuffer channel and worker if required.
	if *config.CollationBufferSize > 1 {
		logger.collateBuffer = make(chan string, *config.CollationBufferSize)

		// Start up a single worker to consume messages from the buffer
		go func() {
			// This is the temporary buffer we'll store logs in.
			logBuffer := []string{}
			for {
				select {
				// Add log to buffer and flush to output if it's full.
				case l := <-logger.collateBuffer:
					logBuffer = append(logBuffer, l)
					if len(logBuffer) >= *config.CollationBufferSize {
						logger.logger.Print(strings.Join(logBuffer, "\n"))
						// Empty buffer
						logBuffer = logBuffer[:0]
					}
				// Flush the buffer to the output after this time, even if we don't fill it.
				case <-time.After(loggerCollateFlushTimeout):
					if len(logBuffer) > 0 {
						logger.logger.Print(strings.Join(logBuffer, "\n"))
						// Empty buffer
						logBuffer = logBuffer[:0]
					}
				}
			}
		}()
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

	// Default to os.Stderr if alternative output is not set
	if lcc.Output == nil {
		lcc.Output = os.Stderr
	}

	// Default to true if 'enabled' is not explicitly set
	if lcc.Enabled == nil {
		lcc.Enabled = BoolPtr(true)
	} else if !*lcc.Enabled {
		// If 'enabled' is explicitly set to false, override the log level and log keys
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

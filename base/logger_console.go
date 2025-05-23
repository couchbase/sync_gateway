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
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// ConsoleLogger is a file logger with a default output of stderr, and tunable log level/keys.
type ConsoleLogger struct {
	FileLogger

	LogLevel     *LogLevel
	LogKeyMask   *LogKeyMask
	ColorEnabled bool

	// isStderr is true when the console logger is enabled with no FileOutput
	isStderr bool

	// ConsoleLoggerConfig stores the initial config used to instantiate ConsoleLogger
	config ConsoleLoggerConfig
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
func NewConsoleLogger(ctx context.Context, shouldLogLocation bool, config *ConsoleLoggerConfig) (*ConsoleLogger, error) {
	if config == nil {
		config = &ConsoleLoggerConfig{}
	}

	cancelCtx, cancelFunc := context.WithCancel(ctx)

	// validate and set defaults
	rotationDoneChan, err := config.init(cancelCtx)
	if err != nil {
		defer cancelFunc()
		return nil, err
	}

	logKey := ToLogKey(ctx, config.LogKeys)

	logger := &ConsoleLogger{
		LogLevel:     config.LogLevel,
		LogKeyMask:   &logKey,
		ColorEnabled: *config.ColorEnabled,
		FileLogger: FileLogger{
			Enabled:          AtomicBool{},
			logger:           log.New(config.Output, "", 0),
			config:           config.FileLoggerConfig,
			rotationDoneChan: rotationDoneChan,
			cancelFunc:       cancelFunc,
			name:             "console",
			closed:           make(chan struct{}),
		},
		isStderr: config.FileOutput == "" && *config.Enabled,
		config:   *config,
	}
	logger.Enabled.Set(*config.Enabled)

	// Only create the collateBuffer channel and worker if required.
	if *config.CollationBufferSize > 1 {
		logger.collateBuffer = make(chan string, *config.CollationBufferSize)
		logger.flushChan = make(chan struct{}, 1)
		logger.collateBufferWg = &sync.WaitGroup{}

		// Start up a single worker to consume messages from the buffer
		go logCollationWorker(logger.closed, logger.collateBuffer, logger.flushChan, logger.collateBufferWg, logger.logger, *config.CollationBufferSize, consoleLoggerCollateFlushTimeout)
	}

	// We can only log the console log location itself when logging has previously been set up and is being re-initialized from a config.
	if shouldLogLocation {
		if *config.Enabled {
			consoleOutput := "stderr"
			if config.FileOutput != "" {
				consoleOutput = config.FileOutput
			}
			ConsolefCtx(ctx, LevelInfo, KeyNone, "Logging: Console to %v", consoleOutput)
		} else {
			ConsolefCtx(ctx, LevelInfo, KeyNone, "Logging: Console disabled")
		}
	}

	return logger, nil
}

func (l *ConsoleLogger) logf(format string, args ...interface{}) {
	if l == nil {
		return
	}
	if l.collateBuffer != nil {
		l.collateBufferWg.Add(1)
		l.collateBuffer <- fmt.Sprintf(format, args...)
	} else {
		l.logger.Printf(format, args...)
	}
}

// shouldLog returns true if the given logLevel and logKey should get logged.
func (l *ConsoleLogger) shouldLog(ctx context.Context, logLevel LogLevel, logKey LogKey) bool {
	if l == nil || l.logger == nil {
		return false
	}

	// database overrides console if set
	shouldLogDb, ok := shouldLogConsoleDatabase(ctx, logLevel, logKey)
	if ok {
		return shouldLogDb
	}

	return shouldLog(l.LogLevel, l.LogKeyMask, logLevel, logKey)
}

// shouldLogConsoleDatabase extracts the database's log settings from the context (if set) to determine whether to log
func shouldLogConsoleDatabase(ctx context.Context, logLevel LogLevel, logKey LogKey) (willLog, ok bool) {
	if ctx == nil {
		return false, false
	}

	logCtx, ok := ctx.Value(requestContextKey).(*LogContext)
	if !ok {
		return false, false
	}

	config := logCtx.DbLogConfig
	if config == nil || config.Console == nil {
		return false, false
	}

	return shouldLog(config.Console.LogLevel, config.Console.LogKeys, logLevel, logKey), true
}

// shouldLog returns true if a log at the given logLineLevel/logLineKey should get logged for the given logger levels/keys.
func shouldLog(loggerLevel *LogLevel, loggerKeys *LogKeyMask, logLineLevel LogLevel, logLineKey LogKey) bool {
	// Log level disabled
	if !loggerLevel.Enabled(logLineLevel) {
		return false
	}

	// Log key All should always log at this point, unless KeyNone is set
	if logLineKey == KeyAll && !loggerKeys.Enabled(KeyNone) {
		return true
	}

	// Finally, check the specific log key is enabled
	return loggerKeys.Enabled(logLineKey)
}

func (l *ConsoleLogger) getConsoleLoggerConfig() *ConsoleLoggerConfig {
	c := ConsoleLoggerConfig{}
	if l != nil {
		// Copy config struct to avoid mutating running config
		c = l.config
	}

	c.FileLoggerConfig = *l.getFileLoggerConfig()
	c.LogLevel = l.LogLevel
	c.LogKeys = l.LogKeyMask.EnabledLogKeys()

	return &c
}

// init validates and sets any defaults for the given ConsoleLoggerConfig
func (lcc *ConsoleLoggerConfig) init(ctx context.Context) (chan struct{}, error) {
	if lcc == nil {
		return nil, errors.New("nil LogConsoleConfig")
	}

	if err := lcc.initRotationConfig("console", 0, 0, nil, false); err != nil {
		return nil, err
	}

	var rotationDoneChan chan struct{}
	// Default to os.Stderr if alternative output is not set
	if lcc.Output == nil && lcc.FileOutput == "" {
		lcc.Output = os.Stderr
	} else if lcc.FileOutput != "" {
		// Otherwise check permissions on the given output and create a Lumberjack logger
		if err := validateLogFileOutput(lcc.FileOutput); err != nil {
			return nil, err
		}
		rotationDoneChan = lcc.initLumberjack(ctx, "console", filepath.FromSlash(lcc.FileOutput))
	}

	// Default to disabled only when a log key or log level has not been specified
	if lcc.Enabled == nil {
		if lcc.LogLevel != nil || len(lcc.LogKeys) > 0 {
			lcc.Enabled = Ptr(true)
		} else {
			lcc.Enabled = Ptr(false)
		}
	}

	// Turn off console logging if disabled
	if !*lcc.Enabled {
		newLevel := LevelNone
		lcc.LogLevel = &newLevel
		lcc.LogKeys = []string{}
	}

	// Default log level
	if lcc.LogLevel == nil {
		newLevel := LevelInfo
		lcc.LogLevel = &newLevel
	} else if *lcc.LogLevel < LevelNone || *lcc.LogLevel > levelCount {
		return nil, fmt.Errorf("invalid log level: %v", *lcc.LogLevel)
	}

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
			if lcc.FileOutput != "" {
				// increase buffer size for file output
				bufferSize = defaultFileLoggerCollateBufferSize
			} else {
				bufferSize = defaultConsoleLoggerCollateBufferSize
			}
		}
		lcc.CollationBufferSize = &bufferSize
	}

	return rotationDoneChan, nil
}

func mustInitConsoleLogger(ctx context.Context, config *ConsoleLoggerConfig) *ConsoleLogger {
	logger, err := NewConsoleLogger(ctx, false, config)
	if err != nil {
		// TODO: CBG-1948
		panic(err)
	}
	return logger
}

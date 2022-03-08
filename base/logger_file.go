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
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/natefinch/lumberjack"
	"github.com/pkg/errors"
)

var (
	ErrInvalidLogFilePath    = errors.New("invalid log file path")
	ErrUnwritableLogFilePath = errors.New("cannot write to log file path directory")

	maxAgeLimit                            = 9999 // days
	defaultMaxSize                         = 100  // 100 MB
	defaultMaxAgeMultiplier                = 2    // e.g. 90 minimum == 180 default maxAge
	defaultCumulativeMaxSizeBeforeDeletion = 1024 // 1 GB
	minRotatedLogsSizeLimit                = 10   // 10 MB
	rotatedLogsLowWatermarkMultiplier      = 0.8  // eg. 100 minRotatedLogsSizeLimit == 80 low watermark

	logFilePrefix = "sg_"

	belowMinValueFmt = "%s for %v was set to %d which is below the minimum of %d"
	aboveMaxValueFmt = "%s for %v was set to %d which is above the maximum of %d"
)

type FileLogger struct {
	Enabled AtomicBool

	// collateBuffer is used to store log entries to batch up multiple logs.
	collateBuffer   chan string
	collateBufferWg *sync.WaitGroup
	flushChan       chan struct{}
	level           LogLevel
	name            string
	output          io.Writer
	logger          *log.Logger
	buffer          strings.Builder

	// FileLoggerConfig stores the initial config used to instantiate FileLogger
	config FileLoggerConfig
}

type FileLoggerConfig struct {
	Enabled  *bool             `json:"enabled,omitempty"`  // Toggle for this log output
	Rotation logRotationConfig `json:"rotation,omitempty"` // Log rotation settings

	CollationBufferSize *int      `json:"collation_buffer_size,omitempty"` // The size of the log collation buffer.
	Output              io.Writer `json:"-"`                               // Logger output. Defaults to os.Stderr. Can be overridden for testing purposes.
}

type logRotationConfig struct {
	MaxSize              *int  `json:"max_size,omitempty"`                // The maximum size in MB of the log file before it gets rotated.
	MaxAge               *int  `json:"max_age,omitempty"`                 // The maximum number of days to retain old log files.
	LocalTime            *bool `json:"localtime,omitempty"`               // If true, it uses the computer's local time to format the backup timestamp.
	RotatedLogsSizeLimit *int  `json:"rotated_logs_size_limit,omitempty"` // Max Size of log files before deletion
}

// NewFileLogger returns a new FileLogger from a config.
func NewFileLogger(config *FileLoggerConfig, level LogLevel, name string, logFilePath string, minAge int, buffer *strings.Builder) (*FileLogger, error) {

	if config == nil {
		config = &FileLoggerConfig{}
	}

	// validate and set defaults
	if err := config.init(level, name, logFilePath, minAge); err != nil {
		return nil, err
	}

	logger := &FileLogger{
		Enabled: AtomicBool{},
		level:   level,
		name:    name,
		output:  config.Output,
		logger:  log.New(config.Output, "", 0),
		config:  *config,
	}
	logger.Enabled.Set(*config.Enabled)

	if buffer != nil {
		logger.buffer = *buffer
	}

	// Only create the collateBuffer channel and worker if required.
	if *config.CollationBufferSize > 1 {
		logger.collateBuffer = make(chan string, *config.CollationBufferSize)
		logger.flushChan = make(chan struct{}, 1)
		logger.collateBufferWg = &sync.WaitGroup{}

		// Start up a single worker to consume messages from the buffer
		go logCollationWorker(logger.collateBuffer, logger.flushChan, logger.collateBufferWg, logger.logger, *config.CollationBufferSize, fileLoggerCollateFlushTimeout)
	}

	return logger, nil
}

func (l *FileLogger) FlushBufferToLog() {
	// Need to clear hanging new line to avoid empty line
	logString := strings.TrimSuffix(l.buffer.String(), "\n")

	// Clear buffer as we no longer need it
	l.buffer.Reset()

	if l.Enabled.IsTrue() {
		l.logf(logString)
	}
}

// Rotate will rotate the active log file.
func (l *FileLogger) Rotate() error {
	if l == nil {
		return errors.New("nil FileLogger")
	}

	if logger, ok := l.output.(*lumberjack.Logger); ok {
		return logger.Rotate()
	}

	return errors.New("can't rotate non-lumberjack log output")
}

func (l *FileLogger) String() string {
	return "FileLogger(" + l.level.String() + ")"
}

// logf will put the given message into the collation buffer if it exists,
// otherwise will log the message directly.
func (l *FileLogger) logf(format string, args ...interface{}) {
	if l.collateBuffer != nil {
		l.collateBufferWg.Add(1)
		l.collateBuffer <- fmt.Sprintf(format, args...)
	} else {
		l.logger.Printf(format, args...)
	}
}

// shouldLog returns true if we can log.
func (l *FileLogger) shouldLog(logLevel LogLevel) bool {
	return l != nil && l.logger != nil &&
		// Check the log file is enabled
		l.Enabled.IsTrue() &&
		// Check the log level is enabled
		l.level >= logLevel
}

func (l *FileLogger) getFileLoggerConfig() *FileLoggerConfig {
	fileLoggerConfig := FileLoggerConfig{}
	if l != nil {
		// Copy config struct to avoid mutating running config
		fileLoggerConfig = l.config
		fileLoggerConfig.Enabled = BoolPtr(l.Enabled.IsTrue())
	}

	return &fileLoggerConfig
}

func (lfc *FileLoggerConfig) init(level LogLevel, name string, logFilePath string, minAge int) error {
	if lfc == nil {
		return errors.New("nil LogFileConfig")
	}

	if lfc.Enabled == nil {
		// enable for all levels less verbose than debug by default
		lfc.Enabled = BoolPtr(level < LevelDebug)
	}

	if err := lfc.initRotationConfig(name, defaultMaxSize, minAge); err != nil {
		return err
	}

	if lfc.Output == nil {
		lfc.Output = newLumberjackOutput(
			filepath.Join(filepath.FromSlash(logFilePath), logFilePrefix+name+".log"),
			*lfc.Rotation.MaxSize,
			*lfc.Rotation.MaxAge,
		)
	}

	if lfc.CollationBufferSize == nil {
		bufferSize := 0
		// Set a default CollationBufferSize for verbose logs.
		if level >= LevelInfo {
			bufferSize = defaultFileLoggerCollateBufferSize
		}
		lfc.CollationBufferSize = &bufferSize
	}

	ticker := time.NewTicker(time.Hour)
	go func() {
		defer func() {
			if panicked := recover(); panicked != nil {
				WarnfCtx(context.Background(), "Panic when deleting rotated log files: \n %s", panicked, debug.Stack())
			}
		}()
		for {
			select {
			case <-ticker.C:
				err := runLogDeletion(logFilePath, level.String(), int(float64(*lfc.Rotation.RotatedLogsSizeLimit)*rotatedLogsLowWatermarkMultiplier), *lfc.Rotation.RotatedLogsSizeLimit)
				if err != nil {
					WarnfCtx(context.Background(), "%s", err)
				}
			}
		}
	}()

	return nil
}

func (lfc *FileLoggerConfig) initRotationConfig(name string, defaultMaxSize, minAge int) error {
	if lfc.Rotation.MaxSize == nil {
		lfc.Rotation.MaxSize = &defaultMaxSize
	} else if *lfc.Rotation.MaxSize == 0 {
		// A value of zero disables the log file rotation in Lumberjack.
	} else if *lfc.Rotation.MaxSize < 0 {
		return fmt.Errorf(belowMinValueFmt, "MaxSize", name, *lfc.Rotation.MaxSize, 0)
	}

	if lfc.Rotation.MaxAge == nil {
		defaultMaxAge := minAge * defaultMaxAgeMultiplier
		lfc.Rotation.MaxAge = &defaultMaxAge
	} else if *lfc.Rotation.MaxAge == 0 {
		// A value of zero disables the age-based log cleanup in Lumberjack.
	} else if *lfc.Rotation.MaxAge < minAge {
		return fmt.Errorf(belowMinValueFmt, "MaxAge", name, *lfc.Rotation.MaxAge, minAge)
	} else if *lfc.Rotation.MaxAge > maxAgeLimit {
		return fmt.Errorf(aboveMaxValueFmt, "MaxAge", name, *lfc.Rotation.MaxAge, maxAgeLimit)
	}

	if lfc.Rotation.RotatedLogsSizeLimit == nil {
		lfc.Rotation.RotatedLogsSizeLimit = &defaultCumulativeMaxSizeBeforeDeletion
	} else if *lfc.Rotation.RotatedLogsSizeLimit < minRotatedLogsSizeLimit {
		return fmt.Errorf(belowMinValueFmt, "RotatedLogsSizeLimit", name, *lfc.Rotation.RotatedLogsSizeLimit, minRotatedLogsSizeLimit)
	}

	return nil
}

func newLumberjackOutput(filename string, maxSize, maxAge int) *lumberjack.Logger {
	return &lumberjack.Logger{
		Filename: filename,
		MaxSize:  maxSize,
		MaxAge:   maxAge,
		Compress: true,
	}
}

// runLogDeletion will delete rotated logs for the supplied logLevel. It will only perform these deletions when the
// cumulative size of the logs are above the supplied sizeLimitMB.
// logDirectory is the supplied directory where the logs are stored.
func runLogDeletion(logDirectory string, logLevel string, sizeLimitMBLowWatermark int, sizeLimitMBHighWatermark int) (err error) {

	sizeLimitMBLowWatermark = sizeLimitMBLowWatermark * 1024 * 1024   //Convert MB input to bytes
	sizeLimitMBHighWatermark = sizeLimitMBHighWatermark * 1024 * 1024 //Convert MB input to bytes

	files, err := ioutil.ReadDir(logDirectory)

	if err != nil {
		return errors.New(fmt.Sprintf("Error reading log directory: %v", err))
	}

	// Traverse backwards through sorted log filenames. When low watermark is reached we record the point at which this
	// was passed. Once we also pass the high watermark we loop through from the low watermark and remove.
	totalSize := 0
	indexDeletePoint := -1
	willDelete := false
	for i := len(files) - 1; i >= 0; i-- {
		file := files[i]
		if strings.HasPrefix(file.Name(), logFilePrefix+logLevel) && strings.HasSuffix(file.Name(), ".log.gz") {
			totalSize += int(file.Size())
			if totalSize > sizeLimitMBLowWatermark && indexDeletePoint == -1 {
				indexDeletePoint = i
			}
			if totalSize > sizeLimitMBHighWatermark {
				willDelete = true
				break
			}
		}
	}

	if willDelete {
		for j := indexDeletePoint; j >= 0; j-- {
			file := files[j]
			if strings.HasPrefix(file.Name(), logFilePrefix+logLevel) && strings.HasSuffix(file.Name(), ".log.gz") {
				err = os.Remove(filepath.Join(logDirectory, file.Name()))
				if err != nil {
					return errors.New(fmt.Sprintf("Error deleting stale log file: %v", err))
				}
			}
		}
	}

	return nil
}

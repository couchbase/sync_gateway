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
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	ErrInvalidLogFilePath    = errors.New("invalid log file path")
	ErrUnwritableLogFilePath = errors.New("cannot write to log file path directory")

	maxAgeLimit                            = 9999               // days
	defaultMaxSize                         = 100                // 100 MB
	defaultMaxAgeMultiplier                = 2                  // e.g. 90 minimum == 180 default maxAge
	defaultCumulativeMaxSizeBeforeDeletion = 1024               // 1 GB
	minRotatedLogsSizeLimit                = 10                 // 10 MB
	rotatedLogsLowWatermarkMultiplier      = 0.8                // eg. 100 minRotatedLogsSizeLimit == 80 low watermark
	minLogRotationInterval                 = 15 * time.Minute   // 15 minutes
	maxLogRotationInterval                 = 7 * 24 * time.Hour // 7 days

	logFilePrefix = "sg_"

	belowMinValueFmt = "%s for %v was set to %v which is below the minimum of %v"
	aboveMaxValueFmt = "%s for %v was set to %v which is above the maximum of %v"

	// lumberjack backupTimeFormat = "2006-01-02T15-04-05.000"
	lumberjackRotationMidfix = `-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}.\d{3}`
	// lumberjack compressSuffix = ".gz"
	optionalCompressSuffix = `(\.gz)?`
)

type FileLogger struct {
	Enabled AtomicBool

	// collateBuffer is used to store log entries to batch up multiple logs.
	collateBuffer    chan string
	collateBufferWg  *sync.WaitGroup
	closed           chan struct{}
	flushChan        chan struct{}
	level            LogLevel
	name             string
	output           io.Writer
	logger           *log.Logger
	buffer           strings.Builder
	cancelFunc       context.CancelFunc // cancelFunc is used to stop the log rotation goroutine
	rotationDoneChan chan struct{}      // rotationDoneChan is used to signal when the log rotation goroutine has stopped

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
	MaxSize              *int            `json:"max_size,omitempty"`                // The maximum size in MB of the log file before it gets rotated.
	MaxAge               *int            `json:"max_age,omitempty"`                 // The maximum number of days to retain old log files.
	LocalTime            *bool           `json:"localtime,omitempty"`               // If true, it uses the computer's local time to format the backup timestamp.
	RotatedLogsSizeLimit *int            `json:"rotated_logs_size_limit,omitempty"` // Max Size of log files before deletion
	RotationInterval     *ConfigDuration `json:"rotation_interval,omitempty"`       // Interval at which logs are rotated
	compress             *bool           `json:"-"`                                 // Compress rotated logs, not exposed to users
}

// NewFileLogger returns a new FileLogger from a config.
func NewFileLogger(ctx context.Context, config *FileLoggerConfig, level LogLevel, name string, logFilePath string, minAge int, defaultMaxAgeOverride *int, buffer *strings.Builder) (*FileLogger, error) {
	if config == nil {
		config = &FileLoggerConfig{}
	}

	cancelCtx, cancelFunc := context.WithCancel(ctx)

	// validate and set defaults
	rotationDoneChan, err := config.init(cancelCtx, level, name, logFilePath, minAge, defaultMaxAgeOverride)
	if err != nil {
		cancelFunc()
		return nil, err
	}

	logger := &FileLogger{
		Enabled:          AtomicBool{},
		level:            level,
		name:             name,
		output:           config.Output,
		logger:           log.New(config.Output, "", 0),
		config:           *config,
		closed:           make(chan struct{}),
		cancelFunc:       cancelFunc,
		rotationDoneChan: rotationDoneChan,
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
		go logCollationWorker(logger.closed, logger.collateBuffer, logger.flushChan, logger.collateBufferWg, logger.logger, *config.CollationBufferSize, fileLoggerCollateFlushTimeout)
	}

	return logger, nil
}

func (l *FileLogger) FlushBufferToLog() {
	// Need to clear hanging new line to avoid empty line
	logString := strings.TrimSuffix(l.buffer.String(), "\n")

	// Clear buffer as we no longer need it
	l.buffer.Reset()

	if l.Enabled.IsTrue() {
		l.log(logString)
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

// Close cancels the log rotation rotation and the underlying file descriptor for the active log file.
func (l *FileLogger) Close() error {
	// cancelFunc will stop the log rotionation/deletion goroutine
	// once all log rotation is done and log output is closed, shut down the logCollationWorker
	defer close(l.closed)
	// cancel the log rotation goroutine and wait for it to stop
	if l.cancelFunc != nil {
		l.cancelFunc()
	}
	// wait for the rotation goroutine to stop
	if l.rotationDoneChan != nil {
		<-l.rotationDoneChan
	}

	if c, ok := l.output.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (l *FileLogger) String() string {
	return "FileLogger(" + l.name + ")"
}

// logf will put the given message into the collation buffer if it exists,
// otherwise will log the message directly.
func (l *FileLogger) logf(format string, args ...interface{}) {
	if l == nil {
		return
	}
	// optimize for the common case of no arguments
	if len(args) == 0 {
		l.log(format)
		return
	}
	if l.collateBuffer != nil {
		l.collateBufferWg.Add(1)
		l.collateBuffer <- fmt.Sprintf(format, args...)
	} else {
		l.logger.Printf(format, args...)
	}
}

// log will put the given message into the collation buffer if it exists,
// otherwise will log the message directly. Use logf for printf style logging.
func (l *FileLogger) log(msg string) {
	if l == nil {
		return
	}
	if l.collateBuffer != nil {
		l.collateBufferWg.Add(1)
		l.collateBuffer <- msg
	} else {
		l.logger.Print(msg)
	}
}

// conditionalPrintf will log the message if the logger is enabled.
func (l *FileLogger) conditionalPrint(msg string) {
	if l.shouldLog(LevelNone) {
		l.log(msg)
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
		fileLoggerConfig.Enabled = Ptr(l.Enabled.IsTrue())
	}

	return &fileLoggerConfig
}

func (lfc *FileLoggerConfig) init(ctx context.Context, level LogLevel, name string, logFilePath string, minAge int, defaultMaxAgeOverride *int) (chan struct{}, error) {
	if lfc == nil {
		return nil, errors.New("nil LogFileConfig")
	}

	if lfc.Enabled == nil {
		// enable for all levels less verbose than debug by default
		lfc.Enabled = Ptr(level < LevelDebug)
	}

	if err := lfc.initRotationConfig(name, defaultMaxSize, minAge, defaultMaxAgeOverride, true); err != nil {
		return nil, err
	}

	var rotationDoneChan chan struct{}
	if lfc.Output == nil {
		logFileName := logFilePrefix + name + ".log"
		logFileOutput := filepath.Join(filepath.FromSlash(logFilePath), logFileName)
		if err := validateLogFileOutput(logFileOutput); err != nil {
			return nil, err
		}
		rotationDoneChan = lfc.initLumberjack(ctx, name, logFileOutput)
	}

	if lfc.CollationBufferSize == nil {
		bufferSize := 0
		// Set a default CollationBufferSize for verbose logs.
		if level >= LevelInfo {
			bufferSize = defaultFileLoggerCollateBufferSize
		}
		lfc.CollationBufferSize = &bufferSize
	}

	return rotationDoneChan, nil
}

// initLumberjack will create a new Lumberjack logger from the given config settings. Returns a doneChan which fires when the log rotation is stopped.
func (lfc *FileLoggerConfig) initLumberjack(ctx context.Context, name string, lumberjackFilename string) chan struct{} {
	rotationDoneChan := make(chan struct{})
	dir, logPattern := getDeletionDirAndRegexp(lumberjackFilename)
	rotateableLogger := &lumberjack.Logger{
		Filename: lumberjackFilename,
		MaxSize:  *lfc.Rotation.MaxSize,
		MaxAge:   *lfc.Rotation.MaxAge,
		Compress: *lfc.Rotation.compress,
	}
	lfc.Output = rotateableLogger

	var rotationTicker *time.Ticker
	var rotationTickerCh <-chan time.Time
	if i := lfc.Rotation.RotationInterval.Value(); i > 0 {
		rotationTicker = time.NewTicker(i)
		rotationTickerCh = rotationTicker.C
	}

	logDeletionTicker := time.NewTicker(rotatedLogDeletionInterval)
	go func() {
		defer func() {
			if panicked := recover(); panicked != nil {
				WarnfCtx(ctx, "Panic when rotating or deleting rotated log files: %s\n%s", panicked, debug.Stack())
			}
		}()
		if rotationTicker != nil {
			defer rotationTicker.Stop()
		}
		defer logDeletionTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				close(rotationDoneChan)
				return
			case <-logDeletionTicker.C:
				err := runLogDeletion(ctx, dir, logPattern, int(float64(*lfc.Rotation.RotatedLogsSizeLimit)*rotatedLogsLowWatermarkMultiplier), *lfc.Rotation.RotatedLogsSizeLimit)
				if err != nil {
					WarnfCtx(ctx, "%s", err)
				}
			case <-rotationTickerCh:
				DebugfCtx(ctx, KeyAll, "Rotating log file %s based on interval %q", name, lfc.Rotation.RotationInterval.Value())
				err := rotateableLogger.Rotate()
				if err != nil {
					WarnfCtx(ctx, "Error rotating log file: %v", err)
				}
			}
		}
	}()
	return rotationDoneChan
}

// initRotationConfig will validate the log rotation settings and set defaults where necessary.
func (lfc *FileLoggerConfig) initRotationConfig(name string, defaultMaxSize, minAge int, defaultMaxAgeOverride *int, compress bool) error {
	if lfc.Rotation.MaxSize == nil {
		lfc.Rotation.MaxSize = &defaultMaxSize
	} else if *lfc.Rotation.MaxSize == 0 {
		// A value of zero disables the log file rotation in Lumberjack.
	} else if *lfc.Rotation.MaxSize < 0 {
		return fmt.Errorf(belowMinValueFmt, "MaxSize", name, *lfc.Rotation.MaxSize, 0)
	}

	if lfc.Rotation.MaxAge == nil {
		if defaultMaxAgeOverride != nil {
			// allows loggers to specify a custom default max age that isn't based on a multiple of minAge
			lfc.Rotation.MaxAge = defaultMaxAgeOverride
		} else {
			// determine based on multiplier of minAge
			defaultMaxAge := minAge * defaultMaxAgeMultiplier
			lfc.Rotation.MaxAge = &defaultMaxAge
		}
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

	if i := lfc.Rotation.RotationInterval; i != nil {
		if i.Value() < minLogRotationInterval {
			return fmt.Errorf(belowMinValueFmt, "RotationInterval", name, i.Value().String(), minLogRotationInterval.String())
		} else if i.Value() > maxLogRotationInterval {
			return fmt.Errorf(aboveMaxValueFmt, "RotationInterval", name, i.Value().String(), maxLogRotationInterval.String())
		}
	}

	if lfc.Rotation.compress == nil {
		lfc.Rotation.compress = &compress
	}
	return nil
}

// runLogDeletion will delete rotated logs for the supplied logLevel. It will only perform these deletions when the
// cumulative size of the logs are above the supplied sizeLimitMB.
// logDirectory is the supplied directory where the logs are stored.
func runLogDeletion(ctx context.Context, logDirectory string, logPattern *regexp.Regexp, sizeLimitMBLowWatermark int, sizeLimitMBHighWatermark int) (err error) {
	sizeLimitMBLowWatermark = sizeLimitMBLowWatermark * 1024 * 1024   // Convert MB input to bytes
	sizeLimitMBHighWatermark = sizeLimitMBHighWatermark * 1024 * 1024 // Convert MB input to bytes

	files, err := os.ReadDir(logDirectory)

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
		if logPattern.Match([]byte(file.Name())) {
			fi, err := file.Info()
			if err != nil {
				InfofCtx(ctx, KeyAll, "Couldn't get size of log file %q: %v - ignoring for cleanup calculation", file.Name(), err)
				continue
			}

			totalSize += int(fi.Size())
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
			if logPattern.Match([]byte(file.Name())) {
				err = os.Remove(filepath.Join(logDirectory, file.Name()))
				if err != nil {
					return errors.New(fmt.Sprintf("Error deleting stale log file: %v", err))
				}
			}
		}
	}

	return nil
}

// getDeletionDirAndRegexp will return the directory and a regexp matching log file and rotated patterns.
func getDeletionDirAndRegexp(path string) (string, *regexp.Regexp) {
	dir, filename := filepath.Split(path)

	// foo
	// foo-2019-01-01T00-00-00.000
	// foo-2019-01-01T00-00-00.000.gz
	filenamePattern := regexp.QuoteMeta(filename)
	rotatedPattern := filenamePattern + lumberjackRotationMidfix + optionalCompressSuffix

	if lastDot := strings.LastIndex(filename, "."); lastDot != -1 {
		// foo.log
		// foo-2019-01-01T00-00-00.000.log
		// foo-2019-01-01T00-00-00.000.log.gz
		//
		// or
		//
		// foo.bar.log
		// foo.bar-2019-01-01T00-00-00.000.log
		// foo.bar-2019-01-01T00-00-00.000.log.gz
		prefix := filename[:lastDot]
		suffix := filename[lastDot:]
		rotatedPattern = regexp.QuoteMeta(prefix) + lumberjackRotationMidfix + regexp.QuoteMeta(suffix) + optionalCompressSuffix
	}

	return dir, regexp.MustCompile(`^(` + filenamePattern + `|` + rotatedPattern + `)$`)
}

package base

import (
	"fmt"
	"io"
	"log"
	"path/filepath"

	"github.com/natefinch/lumberjack"
	"github.com/pkg/errors"
)

var (
	ErrInvalidLogFilePath = errors.New("invalid log file path")

	maxAgeLimit             = 9999 // days
	defaultMaxSize          = 100  // 100 MB
	defaultMaxAgeMultiplier = 2    // e.g. 90 minimum == 180 default maxAge

	belowMinValueFmt = "%s for %v was set to %d which is below the minimum of %d"
	aboveMaxValueFmt = "%s for %v was set to %d which is above the maximum of %d"
)

type FileLogger struct {
	Enabled bool

	// collateBuffer is used to store log entries to batch up multiple logs.
	collateBuffer chan string
	level         LogLevel
	name          string
	output        io.Writer
	logger        *log.Logger
}

type FileLoggerConfig struct {
	Enabled  *bool             `json:"enabled,omitempty"`  // Toggle for this log output
	Rotation logRotationConfig `json:"rotation,omitempty"` // Log rotation settings

	CollationBufferSize *int      `json:"collation_buffer_size,omitempty"` // The size of the log collation buffer.
	Output              io.Writer `json:"-"`                               // Logger output. Defaults to os.Stderr. Can be overridden for testing purposes.
}

type logRotationConfig struct {
	MaxSize   *int `json:"max_size,omitempty"`  // The maximum size in MB of the log file before it gets rotated.
	MaxAge    *int `json:"max_age,omitempty"`   // The maximum number of days to retain old log files.
	LocalTime bool `json:"localtime,omitempty"` // If true, it uses the computer's local time to format the backup timestamp.
}

// NewFileLogger returms a new FileLogger from a config.
func NewFileLogger(config FileLoggerConfig, level LogLevel, name string, logFilePath string, minAge int) (*FileLogger, error) {

	// validate and set defaults
	if err := config.init(level, name, logFilePath, minAge); err != nil {
		return nil, err
	}

	logger := &FileLogger{
		Enabled: *config.Enabled,
		level:   level,
		name:    name,
		output:  config.Output,
		logger:  log.New(config.Output, "", 0),
	}

	// Only create the collateBuffer channel and worker if required.
	if *config.CollationBufferSize > 1 {
		logger.collateBuffer = make(chan string, *config.CollationBufferSize)

		// Start up a single worker to consume messages from the buffer
		go logCollationWorker(logger.collateBuffer, logger.logger, *config.CollationBufferSize)
	}

	return logger, nil
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

func (l FileLogger) String() string {
	return "FileLogger(" + l.level.String() + ")"
}

// logf will put the given message into the collation buffer if it exists,
// otherwise will log the message directly.
func (l *FileLogger) logf(format string, args ...interface{}) {
	if l.collateBuffer != nil {
		l.collateBuffer <- fmt.Sprintf(format, args...)
	} else {
		l.logger.Printf(format, args...)
	}
}

// shouldLog returns true if we can log.
func (l *FileLogger) shouldLog(logLevel LogLevel) bool {
	return l != nil && l.logger != nil &&
		// Check the log file is enabled
		l.Enabled &&
		// Check the log level is enabled
		l.level >= logLevel
}

func (lfc *FileLoggerConfig) init(level LogLevel, name string, logFilePath string, minAge int) error {
	if lfc == nil {
		return errors.New("nil LogFileConfig")
	}

	if lfc.Enabled == nil {
		// enable for all levels except debug by default
		lfc.Enabled = BoolPtr(level != LevelDebug)
	}

	if err := lfc.initRotationConfig(name, defaultMaxSize, minAge); err != nil {
		return err
	}

	if lfc.Output == nil {
		lfc.Output = newLumberjackOutput(
			filepath.Join(filepath.FromSlash(logFilePath), "sg_"+name+".log"),
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

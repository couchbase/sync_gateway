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
	ErrInvalidLogFilePath   = errors.New("Invalid LogFilePath")
	ErrInvalidLoggingMaxAge = errors.New("Invalid MaxAge")

	maxAgeLimit             = 9999 // days
	maxSizeLimit            = 2048 // 2 GB
	defaultMaxSize          = 100  // 100 MB
	defaultMaxAgeMultiplier = 2    // e.g. 90 minimum == 180 default maxAge
)

type FileLogger struct {
	Enabled bool

	level  LogLevel
	output io.Writer
	logger *log.Logger
}

type FileLoggerConfig struct {
	Enabled  *bool             `json:"enabled,omitempty"`  // Toggle for this log output
	Rotation logRotationConfig `json:"rotation,omitempty"` // Log rotation settings

	Output io.Writer `json:"-"` // Logger output. Defaults to os.Stderr. Can be overridden for testing purposes.
}

type logRotationConfig struct {
	MaxSize   *int `json:"max_size,omitempty"`  // The maximum size in MB of the log file before it gets rotated.
	MaxAge    *int `json:"max_age,omitempty"`   // The maximum number of days to retain old log files.
	LocalTime bool `json:"localtime,omitempty"` // If true, it uses the computer's local time to format the backup timestamp.
}

// NewFileLogger returms a new FileLogger from a config.
func NewFileLogger(config FileLoggerConfig, level LogLevel, logFilePath string, minAge int) (*FileLogger, error) {
	// validate and set defaults
	if err := config.init(level, logFilePath, minAge); err != nil {
		return nil, err
	}

	return &FileLogger{
		Enabled: *config.Enabled,
		level:   level,
		output:  config.Output,
		logger:  log.New(config.Output, "", 0),
	}, nil
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

// shouldLog returns true if we can log.
func (l *FileLogger) shouldLog() bool {
	return l != nil && l.logger != nil &&
		// Check the log file is enabled
		l.Enabled
}

func (lfc *FileLoggerConfig) init(level LogLevel, logFilePath string, minAge int) error {
	if lfc == nil {
		return errors.New("nil LogFileConfig")
	}

	if lfc.Enabled == nil {
		// enable for all levels except debug by default
		lfc.Enabled = BoolPtr(level != LevelDebug)
	}

	if lfc.Rotation.MaxSize == nil {
		lfc.Rotation.MaxSize = &defaultMaxSize
	} else if *lfc.Rotation.MaxSize > maxSizeLimit {
		return fmt.Errorf("MaxSize for %v was set to %d which is above the maximum of %d", level, *lfc.Rotation.MaxSize, maxSizeLimit)
	}

	if lfc.Rotation.MaxAge == nil {
		defaultMaxAge := minAge * defaultMaxAgeMultiplier
		lfc.Rotation.MaxAge = &defaultMaxAge
	} else if *lfc.Rotation.MaxAge == 0 {
		// A value of zero disables the age-based log cleanup in Lumberjack.
	} else if *lfc.Rotation.MaxAge < minAge {
		return fmt.Errorf("MaxAge for %v was set to %d which is below the minimum of %d", level, *lfc.Rotation.MaxAge, minAge)
	} else if *lfc.Rotation.MaxAge > maxAgeLimit {
		return fmt.Errorf("MaxAge for %v was set to %d which is above the maximum of %d", level, *lfc.Rotation.MaxAge, maxAgeLimit)
	}

	if lfc.Output == nil {
		lfc.Output = newLumberjackOutput(
			filepath.Join(filepath.FromSlash(logFilePath), "sg_"+level.String()+".log"),
			*lfc.Rotation.MaxSize,
			*lfc.Rotation.MaxAge,
		)
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

package base

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/natefinch/lumberjack"
	"github.com/pkg/errors"
)

var (
	ErrInvalidLogFilePath   = errors.New("Invalid LogFilePath")
	ErrInvalidLoggingMaxAge = errors.New("Invalid MaxAge")
)

type FileLogger struct {
	Enabled bool

	logger *log.Logger
}

type FileLoggerConfig struct {
	Enabled  *bool             `json:",omitempty"` // Toggle for this log output
	Rotation logRotationConfig `json:",omitempty"` // Log rotation settings

	Output io.Writer `json:"-"` // Logger output. Defaults to os.Stderr. Can be overridden for testing purposes.
}

type logRotationConfig struct {
	MaxSize   *int `json:",omitempty"` // The maximum size in MB of the log file before it gets rotated.
	MaxAge    *int `json:",omitempty"` // The maximum number of days to retain old log files.
	LocalTime bool `json:",omitempty"` // If true, it uses the computer's local time to format the backup timestamp.
}

// NewFileLogger returms a new FileLogger from a config.
func NewFileLogger(config FileLoggerConfig, level LogLevel, logFilePath string, minAge int) (*FileLogger, error) {
	// validate and set defaults
	if err := config.init(level, logFilePath, minAge); err != nil {
		return nil, err
	}

	return &FileLogger{
		Enabled: *config.Enabled,
		logger:  log.New(config.Output, "", 0),
	}, nil
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

	// set default enabled based on level
	if lfc.Enabled == nil {
		// enable for all levels except debug
		lfc.Enabled = BoolPtr(level != LevelDebug)
	}

	if lfc.Rotation.MaxSize == nil {
		defaultMaxSize := 200
		lfc.Rotation.MaxSize = &defaultMaxSize
	}

	if lfc.Rotation.MaxAge == nil {
		// Default to double the minAge
		defaultMaxAge := minAge * 2
		lfc.Rotation.MaxAge = &defaultMaxAge
	} else if *lfc.Rotation.MaxAge < minAge {
		return fmt.Errorf("MaxAge for %s was set to %d which is below the minimum of %d", LogLevelName(level), *lfc.Rotation.MaxAge, minAge)
	}

	lfc.Output = newLumberjackOutput(
		filepath.Join(filepath.FromSlash(logFilePath), "sg_"+LogLevelName(level)+".log"),
		*lfc.Rotation.MaxSize,
		*lfc.Rotation.MaxAge,
	)

	return nil
}

func validateLogFilePath(logFilePath *string) error {
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

func newLumberjackOutput(filename string, maxSize, maxAge int) *lumberjack.Logger {
	return &lumberjack.Logger{
		Filename: filename,
		MaxSize:  maxSize,
		MaxAge:   maxAge,
		Compress: true,
	}
}

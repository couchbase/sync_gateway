//  Copyright (c) 2012-2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/clog"
	"github.com/couchbase/goutils/logging"
	"github.com/natefinch/lumberjack"
)

var errMarshalNilLevel = errors.New("can't marshal a nil *Level to text")

const (
	// If true, all HTTP request/response bodies will be logged.
	// Use this sparingly as it will probably dump sensitive information into the logs.
	EnableLogHTTPBodies = false
)

type Level int32

//By setting DebugLevel to -1, if LogLevel is not set in the logging config it
//will default to the zero value for int32 (0) which will disable debug
//logging, InfoLevel logging will be the default output.
const (
	// DebugLevel logs are typically voluminous, and are usually disabled in
	// production.
	DebugLevel Level = iota - 1
	// InfoLevel is the default logging priority.
	InfoLevel
	// WarnLevel logs are more important than Info, but don't need individual
	// human review.
	WarnLevel
	// ErrorLevel logs are high-priority. If an application is running smoothly,
	// it shouldn't generate any error-level logs.
	ErrorLevel
	// PanicLevel logs a message, then panics.
	PanicLevel
	// FatalLevel logs a message, then calls os.Exit(1).
	FatalLevel
)

// sgLevel returns a compatible internal SyncGateway Log Level for
// the given logging config Level
// The mapping is:
//
// DebugLevel	-1 --> 1
// InfoLevel 	 0 --> 1
// WarnLevel 	 1 --> 2
// ErrorLevel 	 2 --> 2
// PanicLevel 	 3 --> 3
// FatalLevel 	 4 --> 3
//
// This can be mapped by addition of 2 to level value
// and then a division by 2 and return the ceil of the result
// to round to nearest int sgLevel value
func (l Level) sgLevel() int {
	return int(math.Ceil(float64(l+2) / float64(2)))
}

// cgLevel returns a compatible go-couchbase/golog Log Level for
// the given logging config Level
// The mapping is:
//
// DebugLevel	-1 --> 7
// InfoLevel 	 0 --> 4
// WarnLevel 	 1 --> 3
// ErrorLevel 	 2 --> 2
// PanicLevel 	 3 --> 1
// FatalLevel 	 4 --> 0
func (l Level) cgLevel() logging.Level {
	switch l {
	case DebugLevel:
		return logging.DEBUG
	case InfoLevel:
		return logging.INFO
	case WarnLevel:
		return logging.WARN
	case ErrorLevel:
		return logging.ERROR
	case PanicLevel:
		return logging.SEVERE
	case FatalLevel:
		return logging.SEVERE
	default:
		return logging.NONE
	}
}

// String returns a lower-case ASCII representation of the log level.
func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "debug"
	case InfoLevel:
		return "info"
	case WarnLevel:
		return "warn"
	case ErrorLevel:
		return "error"
	case PanicLevel:
		return "panic"
	case FatalLevel:
		return "fatal"
	default:
		return fmt.Sprintf("Level(%d)", l)
	}
}

// Implementaion of the go encoding.TextMarshaller interface for the Level type
// This method will also be called by the JSON Marshaller
//
// MarshalText marshals the Level to text. Note that the text representation
// drops the -Level suffix (see example).
func (l *Level) MarshalText() ([]byte, error) {
	if l == nil {
		return nil, errMarshalNilLevel
	}
	return []byte(l.String()), nil
}

// Implementaion of the go encoding.TextUnmarshaller interface for the Level type
// This method will also be called by the JSON Unmarshaller e.g. when loading
// from logging configuration.
//
// UnmarshalText unmarshals text to a level. Like MarshalText, UnmarshalText
// expects the text representation of a Level to drop the -Level suffix (see
// example).
//
// In particular, this makes it easy to configure logging levels using YAML,
// TOML, or JSON files.
func (l *Level) UnmarshalText(text []byte) error {
	switch string(text) {
	case "debug":
		*l = DebugLevel
	case "info":
		*l = InfoLevel
	case "warn":
		*l = WarnLevel
	case "error":
		*l = ErrorLevel
	case "panic":
		*l = PanicLevel
	case "fatal":
		*l = FatalLevel
	default:
		return fmt.Errorf("unrecognized level: %v", string(text))
	}
	return nil
}

// 1 enables regular logs, 2 enables warnings, 3+ is nothing but panics.
// Default value is 1.
var logLevel int = 1

// Set of LogTo() key strings that are enabled.
var LogKeys map[string]bool

var logNoTime bool

var logLock sync.RWMutex

var logger *log.Logger

var logFile *os.File

var logStar bool // enabling log key "*" enables all key-based logging

type LogRotationConfig struct {
	// MaxSize is the maximum size in megabytes of the log file before it gets
	// rotated. It defaults to 100 megabytes.
	MaxSize int `json:",omitempty"`

	// MaxAge is the maximum number of days to retain old log files based on the
	// timestamp encoded in their filename.  Note that a day is defined as 24
	// hours and may not exactly correspond to calendar days due to daylight
	// savings, leap seconds, etc. The default is not to remove old log files
	// based on age.
	MaxAge int `json:",omitempty"`

	// MaxBackups is the maximum number of old log files to retain.  The default
	// is to retain all old log files (though MaxAge may still cause them to get
	// deleted.)
	MaxBackups int `json:",omitempty"`

	// LocalTime determines if the time used for formatting the timestamps in
	// backup files is the computer's local time.  The default is to use UTC
	// time.
	LocalTime bool `json:",omitempty"`
	// contains filtered or unexported fields
}

type LogAppenderConfig struct {
	// Filename is the file to write logs to.  Backup log files will be retained
	// in the same directory.  It uses <processname>-lumberjack.log in
	// os.TempDir() if empty.
	LogFilePath    *string            `json:",omitempty"`
	LogKeys        []string           `json:",omitempty"` // Log keywords to enable
	LogLevel       Level              `json:",omitempty"`
	Rotation       *LogRotationConfig `json:",omitempty"`
	RedactionLevel RedactionLevel     `json:",omitempty"`
}

type LoggingConfigMap map[string]*LogAppenderConfig

//Attach logger to stderr during load, this may get re-attached once config is loaded
func init() {
	logger = log.New(os.Stderr, "", 0)
	LogKeys = make(map[string]bool)
	logNoTime = false
}

func GetLogLevel() int {
	return logLevel
}

func SetLogLevel(level int) {
	logLock.Lock()
	defer logLock.Unlock()
	logLevel = level
}

// For transforming a new log level to the old type.
func ToDeprecatedLogLevel(logLevel LogLevel) *Level {
	var deprecatedLogLevel Level
	switch logLevel {
	case LevelDebug:
		deprecatedLogLevel = DebugLevel
	case LevelInfo:
		deprecatedLogLevel = InfoLevel
	case LevelWarn:
		deprecatedLogLevel = WarnLevel
	case LevelError:
		deprecatedLogLevel = ErrorLevel
	}
	return &deprecatedLogLevel
}

// For transforming an old log level to the new type.
func ToLogLevel(deprecatedLogLevel Level) *LogLevel {
	var newLogLevel LogLevel
	switch deprecatedLogLevel {
	case DebugLevel:
		newLogLevel.Set(LevelDebug)
	case InfoLevel:
		newLogLevel.Set(LevelInfo)
	case WarnLevel:
		newLogLevel.Set(LevelWarn)
	case ErrorLevel:
		newLogLevel.Set(LevelError)
	}
	return &newLogLevel
}

// Disables ANSI color in log output.
func LogNoColor() {
	// this is now the default state; see LogColor() below
}

func LogNoTime() {
	logLock.RLock()
	defer logLock.RUnlock()
	//Disable timestamp for default logger, this may be used by other packages
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime | log.Lmicroseconds))
	logNoTime = true
}

func LogTime() {
	logLock.RLock()
	defer logLock.RUnlock()
	//Enable timestamp for default logger, this may be used by other packages
	log.SetFlags(log.Flags() | (log.Ldate | log.Ltime | log.Lmicroseconds))
	logNoTime = false
}

// Parses a comma-separated list of log keys, probably coming from an argv flag.
// The key "bw" is interpreted as a call to LogNoColor, not a key.
func ParseLogFlag(flag string) {
	if flag != "" {
		ParseLogFlags(strings.Split(flag, ","))
	}
}

func (config *LogAppenderConfig) ValidateLogAppender() error {
	//Fail validation if an appender contains a "rotation" sub document
	// and no "logFilePath" appender property is defined
	if config.Rotation != nil {
		if config.LogFilePath == nil {
			return fmt.Errorf("The default logger must define a \"logFilePath\" when \"rotation\" is defined")
		}
		if _, err := IsFilePathWritable(*config.LogFilePath); err != nil {
			return err
		}
		if config.Rotation.MaxSize < 0 {
			return fmt.Errorf("Log rotation MaxSize must >= 0")
		}
		if config.Rotation.MaxAge < 0 {
			return fmt.Errorf("Log rotation MaxAge must >= 0")
		}
		if config.Rotation.MaxBackups < 0 {
			return fmt.Errorf("Log rotation MaxBackups must >= 0")
		}
	}

	return nil
}

// Parses an array of log keys, probably coming from a argv flags.
// The key "bw" is interpreted as a call to LogNoColor, not a key.
func ParseLogFlags(flags []string) {
	logLock.Lock()
	keyMap := make(map[string]bool)

	for _, key := range flags {
		keyMap[key] = true
	}

	ParseLogFlagsMap(keyMap)
	logLock.Unlock()
	Infof(KeyAll, "Enabling logging: %s", flags)
}

// Parses a map of log keys and enabled bool, probably coming from a argv flags.
// The key "bw" is interpreted as a call to LogNoColor, not a key.
func ParseLogFlagsMap(flags map[string]bool) {
	for key, enabled := range flags {
		switch key {
		case "bw":
			if enabled {
				LogNoColor()
			} else {
				LogColor()
			}
		case "color":
			if enabled {
				LogColor()
			} else {
				LogNoColor()
			}
		case "notime":
			if enabled {
				LogNoTime()
			} else {
				LogTime()
			}
		default:
			if enabled {
				LogKeys[key] = enabled
				for strings.HasSuffix(key, "+") {
					key = key[0 : len(key)-1]
					LogKeys[key] = enabled // "foo+" also enables "foo"
				}
			} else {
				delete(LogKeys, key)
				//if key already has "++" suffix there is no further processing
				// else if it has "+" suffix disable "++" suffix as well
				// else disable "+" suffix and "++" suffix
				if !strings.HasSuffix(key, "++") {
					//If key does not have a "+" suffix then remove "+" suffix
					if !strings.HasSuffix(key, "+") {
						// remove the "+" suffix as well
						delete(LogKeys, key+"+")
						// remove the "++" suffix as well
						delete(LogKeys, key+"++")
					} else {
						// remove the "++" suffix as well
						delete(LogKeys, key+"+")
					}
				}
			}
			if key == "*" {
				logStar = enabled
				if enabled {
					EnableSgReplicateLogging()
				}
			}
			// gocb requires a call into the gocb library to enable logging
			if key == "gocb" {
				if enabled {
					EnableGoCBLogging()
				} else {
					DisableGoCBLogging()
				}
			}
			if key == "Replicate" {
				if enabled {
					EnableSgReplicateLogging()
				} else {
					DisableSgReplicateLogging()
				}
			}
		}
	}
}

func EnableSgReplicateLogging() {
	clog.EnableKey("Replicate")
}

func DisableSgReplicateLogging() {
	clog.DisableKey("Replicate")
}

func GetLogKeys() map[string]bool {
	logLock.RLock()
	defer logLock.RUnlock()
	keys := map[string]bool{}
	for k, v := range LogKeys {
		keys[k] = v
	}
	return keys
}

func UpdateLogKeys(keys map[string]bool, replace bool) {
	logLock.Lock()
	defer logLock.Unlock()
	if replace {
		LogKeys = map[string]bool{}
	}

	ParseLogFlagsMap(keys)
}

// Returns a string identifying a function on the call stack.
// Use depth=1 for the caller of the function that calls GetCallersName, etc.
func GetCallersName(depth int) string {
	pc, file, line, ok := runtime.Caller(depth + 1)
	if !ok {
		return "???"
	}

	fnname := ""
	if fn := runtime.FuncForPC(pc); fn != nil {
		fnname = fn.Name()
	}

	return fmt.Sprintf("%s() at %s:%d", lastComponent(fnname), lastComponent(file), line)
}

// Partial interface for the SGLogger
type SGLogger interface {
	Logf(logLevel LogLevel, logKey LogKey, format string, args ...interface{})
}

func EnableLogKey(key string) {
	logLock.Lock()
	defer logLock.Unlock()
	LogKeys[key] = true
}

func DisableLogKey(key string) {
	logLock.Lock()
	defer logLock.Unlock()
	LogKeys[key] = false
}

func LogEnabled(key string) bool {
	logLock.RLock()
	defer logLock.RUnlock()
	return logStar || LogKeys[key]
}

func LogEnabledExcludingLogStar(key string) bool {
	logLock.RLock()
	defer logLock.RUnlock()
	return LogKeys[key]
}

// Logs a message to the console.
func Log(message string) {
	logLock.RLock()
	defer logLock.RUnlock()
	ok := logLevel <= 1

	if ok {
		print(message)
	}
}

// Logs a formatted message to the console.
func Logf(format string, args ...interface{}) {
	logLock.RLock()
	defer logLock.RUnlock()
	ok := logLevel <= 1

	if ok {
		printf(format, args...)
	}
}

// If the error is not nil, logs its description and the name of the calling function.
// Returns the input error for easy chaining.
func LogError(err error) error {
	if err != nil {
		logLock.RLock()
		ok := logLevel <= 2
		logLock.RUnlock()

		if ok {
			logWithCaller(fgRed, "ERROR", "%v", err)
		}
	}
	return err
}

// Logs a warning to the console
func Warn(format string, args ...interface{}) {
	logLock.RLock()
	ok := logLevel <= 2
	logLock.RUnlock()

	if ok {
		logWithCaller(fgRed, "WARNING", format, args...)
	}
}

// Logs a highlighted message prefixed with "TEMP". This function is intended for
// temporary logging calls added during development and not to be checked in, hence its
// distinctive name (which is visible and easy to search for before committing.)
func TEMP(format string, args ...interface{}) {
	logWithCaller(fgYellow, "TEMP", format, args...)
}

// Logs a warning to the console, then panics.
func LogPanic(format string, args ...interface{}) {
	logWithCaller(fgRed, "PANIC", format, args...)
	panic(fmt.Sprintf(format, args...))
}

// Logs a warning to the console, then exits the process.
func LogFatal(format string, args ...interface{}) {
	logWithCaller(fgRed, "FATAL", format, args...)
	os.Exit(1)
}

func logWithCaller(color string, prefix string, format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	logLock.RLock()
	defer logLock.RUnlock()
	print(color, prefix, ": ", message, reset,
		dim, " -- ", GetCallersName(2), reset)
}

// Simple wrapper that converts Print to Printf.  Assumes caller is holding logLock read lock.
func print(args ...interface{}) {
	ok := logLevel <= 1

	if ok {
		printf("%s", fmt.Sprint(args...))
	}
}

// Logs a formatted message to the underlying logger.  Assumes caller is holding logLock read lock.
func printf(format string, args ...interface{}) {
	ok := logLevel <= 1

	if ok {
		if !logNoTime {
			timestampedFormat := strings.Join([]string{time.Now().Format(ISO8601Format), format}, " ")
			logger.Printf(timestampedFormat, args...)
		} else {
			logger.Printf(format, args...)
		}
	}
}

func lastComponent(path string) string {
	if index := strings.LastIndex(path, "/"); index >= 0 {
		path = path[index+1:]
	} else if index = strings.LastIndex(path, "\\"); index >= 0 {
		path = path[index+1:]
	}
	return path
}

func UpdateLogger(logFilePath string) {
	//Attempt to open file for write at path provided
	fo, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	if err != nil {
		LogFatal("unable to open logfile for write: %s", logFilePath)
	}

	//defer write lock to here otherwise LogFatal above will deadlock
	logLock.Lock()

	//We keep a reference to the underlying log File as log.Logger and io.Writer
	//have no close() methods and we want to close old files on log rotation
	oldLogFile := logFile
	logFile = fo
	logger = log.New(fo, "", log.Lmicroseconds)
	logLock.Unlock()

	//re-apply log no time flags on new logger
	if logNoTime {
		LogNoTime()
	}

	//If there is a previously opened log file, explicitly close it
	if oldLogFile != nil {
		err = oldLogFile.Close()
		if err != nil {
			Warn("unable to close old log File after updating logger")
		}
	}
}

// This provides an io.Writer interface around the base.Infof API
type LoggerWriter struct {
	LogKey       LogKey        // The log key to log to, eg, KeyHTTP
	SerialNumber uint64        // The request ID
	Request      *http.Request // The request
}

// Write() method to satisfy the io.Writer interface
func (lw *LoggerWriter) Write(p []byte) (n int, err error) {
	Infof(lw.LogKey, " #%03d: %s %s %s", lw.SerialNumber, lw.Request.Method, SanitizeRequestURL(lw.Request.URL), string(p))
	return len(p), nil
}

// Create a new LoggerWriter
func NewLoggerWriter(logKey LogKey, serialNumber uint64, req *http.Request) *LoggerWriter {
	return &LoggerWriter{
		LogKey:       logKey,
		SerialNumber: serialNumber,
		Request:      req,
	}
}

func CreateRollingLogger(logConfig *LogAppenderConfig) {
	if logConfig != nil {
		SetLogLevel(logConfig.LogLevel.sgLevel())
		ParseLogFlags(logConfig.LogKeys)

		if logConfig.LogFilePath == nil {
			return
		}

		lj := lumberjack.Logger{}

		lj.Filename = *logConfig.LogFilePath
		log.Printf("Log entries will be written to the file %v", *logConfig.LogFilePath)

		if rotation := logConfig.Rotation; rotation != nil {
			if rotation.MaxSize > 0 {
				lj.MaxSize = rotation.MaxSize // megabytes
			}
			if rotation.MaxAge > 0 {
				lj.MaxAge = rotation.MaxAge
			}
			if rotation.MaxBackups > 0 {
				lj.MaxBackups = rotation.MaxBackups
			}
			lj.LocalTime = rotation.LocalTime
		}

		//Update default GoLang logger to use new rolling logger
		logger.SetOutput(&lj)

		//Update sg_replicate logger to use rolling logger
		clog.SetOutput(&lj)

		//Update go-couchbase to use rolling logger
		gcblogger := logging.NewLogger(&lj, logConfig.LogLevel.cgLevel(), logging.TEXTFORMATTER)
		logging.SetLogger(gcblogger)
	}
}

// ANSI color control escape sequences.
// Shamelessly copied from https://github.com/sqp/godock/blob/master/libs/log/colors.go
var (
	reset,
	bright,
	dim,
	underscore,
	blink,
	reverse,
	hidden,
	fgBlack,
	fgRed,
	fgGreen,
	fgYellow,
	fgBlue,
	fgMagenta,
	fgCyan,
	fgWhite,
	bgBlack,
	bgRed,
	bgGreen,
	bgYellow,
	bgBlue,
	bgMagenta,
	bgCyan,
	bgWhite string
)

func LogColor() {
	reset = "\x1b[0m"
	bright = "\x1b[1m"
	dim = "\x1b[2m"
	underscore = "\x1b[4m"
	blink = "\x1b[5m"
	reverse = "\x1b[7m"
	hidden = "\x1b[8m"
	fgBlack = "\x1b[30m"
	fgRed = "\x1b[31m"
	fgGreen = "\x1b[32m"
	fgYellow = "\x1b[33m"
	fgBlue = "\x1b[34m"
	fgMagenta = "\x1b[35m"
	fgCyan = "\x1b[36m"
	fgWhite = "\x1b[37m"
	bgBlack = "\x1b[40m"
	bgRed = "\x1b[41m"
	bgGreen = "\x1b[42m"
	bgYellow = "\x1b[43m"
	bgBlue = "\x1b[44m"
	bgMagenta = "\x1b[45m"
	bgCyan = "\x1b[46m"
	bgWhite = "\x1b[47m"
}

// Prepend a context ID to each blip logging message.  The contextID uniquely identifies the blip context, and
// is useful for grouping the blip connections in the log output.
func PrependContextID(contextID, format string, params ...interface{}) (newFormat string, newParams []interface{}) {

	// Add a new format placeholder for the context ID, which should appear at the beginning of the logs
	formatWithContextID := `[%s] ` + format

	params = append(params, 0)
	copy(params[1:], params[0:])
	params[0] = contextID

	return formatWithContextID, params

}

// *************************************************************************
//   2018-04-10: New logging below. Above code is to be removed/cleaned up
// *************************************************************************

var (
	consoleLogger                                    *ConsoleLogger
	debugLogger, infoLogger, warnLogger, errorLogger *FileLogger
)

func init() {
	// We'll initilise a default consoleLogger so we can still log stuff before/during parsing logging configs.
	// This maintains consistent formatting (timestamps, levels, etc) in the output,
	// and allows a single set of logging functions to be used, rather than fmt.Printf()
	consoleLogger = newConsoleLoggerOrPanic(&ConsoleLoggerConfig{})
}

// Panicf logs the given formatted string and args to the error log level and given log key and then panics.
func Panicf(logKey LogKey, format string, args ...interface{}) {
	Errorf(logKey, format, args...)
	panic(fmt.Sprintf(format, args...))
}

// Fatalf logs the given formatted string and args to the error log level and given log key and then exits.
func Fatalf(logKey LogKey, format string, args ...interface{}) {
	Errorf(logKey, format, args...)
	os.Exit(1)
}

// Errorf logs the given formatted string and args to the error log level and given log key.
func Errorf(logKey LogKey, format string, args ...interface{}) {
	logTo(LevelError, logKey, format, args...)
}

// Warnf logs the given formatted string and args to the warn log level and given log key.
func Warnf(logKey LogKey, format string, args ...interface{}) {
	logTo(LevelWarn, logKey, format, args...)
}

// Infof logs the given formatted string and args to the info log level and given log key.
func Infof(logKey LogKey, format string, args ...interface{}) {
	logTo(LevelInfo, logKey, format, args...)
}

// Debugf logs the given formatted string and args to the debug log level with an optional log key.
func Debugf(logKey LogKey, format string, args ...interface{}) {
	logTo(LevelDebug, logKey, format, args...)
}

func logTo(logLevel LogLevel, logKey LogKey, format string, args ...interface{}) {
	shouldLogConsole := consoleLogger.shouldLog(logLevel, logKey)
	shouldLogError := errorLogger.shouldLog()
	shouldLogWarn := warnLogger.shouldLog()
	shouldLogInfo := infoLogger.shouldLog()
	shouldLogDebug := debugLogger.shouldLog()

	shouldLog := shouldLogConsole || shouldLogError || shouldLogWarn || shouldLogInfo || shouldLogDebug

	// exit early if we aren't going to log anything
	if !shouldLog || logLevel <= LevelNone {
		return
	}

	// Prepend timestamp, level, log key
	format = addPrefixes(format, logLevel, logKey)

	// Warn and error logs also append caller name/line numbers.
	if logLevel <= LevelWarn {
		format += " -- " + GetCallersName(2)
	}

	// Perform log redaction, if necessary.
	args = redact(args)

	if shouldLogConsole {
		consoleLogger.logger.Printf(color(format, logLevel), args...)
	}

	switch logLevel {
	case LevelError:
		if shouldLogError {
			errorLogger.logger.Printf(format, args...)
		}
		fallthrough
	case LevelWarn:
		if shouldLogWarn {
			warnLogger.logger.Printf(format, args...)
		}
		fallthrough
	case LevelInfo:
		if shouldLogInfo {
			infoLogger.logger.Printf(format, args...)
		}
		fallthrough
	case LevelDebug:
		if shouldLogDebug {
			debugLogger.logger.Printf(format, args...)
		}
	}
}

// Broadcastf will print the same log to ALL outputs, ignoring logLevel and logKey settings.
// This can be useful for printing an indicator of app restarts, version numbers, etc. but MUST be used sparingly.
func Broadcastf(format string, args ...interface{}) {
	format = addPrefixes(format, LevelNone, KeyNone)
	if consoleLogger.logger != nil {
		consoleLogger.logger.Printf(color(format, LevelNone), args...)
	}
	if errorLogger.shouldLog() {
		errorLogger.logger.Printf(format, args...)
	}
	if warnLogger.shouldLog() {
		warnLogger.logger.Printf(format, args...)
	}
	if infoLogger.shouldLog() {
		infoLogger.logger.Printf(format, args...)
	}
	if debugLogger.shouldLog() {
		debugLogger.logger.Printf(format, args...)
	}
}

// addPrefixes will modify the format string to add timestamps, log level, and other common prefixes.
func addPrefixes(format string, logLevel LogLevel, logKey LogKey) string {
	timestampPrefix := time.Now().Format(ISO8601Format) + " "

	var logLevelPrefix string
	if logLevel > LevelNone {
		logLevelPrefix = "[" + logLevelNamePrint(logLevel) + "] "
	}

	var logKeyPrefix string
	if logKey > KeyNone && logKey != KeyAll {
		logKeyName := LogKeyName(logKey)
		// Append "+" to logKeys at debug level (for backwards compatibility)
		if logLevel == LevelDebug {
			logKeyName += "+"
		}
		logKeyPrefix = logKeyName + ": "
	}

	return timestampPrefix + logLevelPrefix + logKeyPrefix + format
}

// color wraps the given string with color based on logLevel
// This won't work on Windows. Maybe use fatih's colour package?
func color(str string, logLevel LogLevel) string {
	if !colorEnabled() {
		return str
	}

	var color string

	switch logLevel {
	case LevelError:
		color = "\033[1;31m"
	case LevelWarn:
		color = "\033[1;33m"
	case LevelInfo:
		color = "\033[1;34m"
	case LevelDebug:
		color = "\033[0;36m"
	case LevelNone:
		color = "\033[0;32m"
	}

	return color + str + "\033[0m"
}

func colorEnabled() bool {
	return consoleLogger.ColorEnabled &&
		os.Getenv("TERM") != "dumb" &&
		runtime.GOOS != "windows"
}

// LogDebugEnabled returns true if either the console should log at debug level,
// or if the debugLogger is enabled.
func LogDebugEnabled() bool {
	return consoleLogger.shouldLog(LevelDebug, KeyAll) || debugLogger.shouldLog()
}

// ConsoleLogLevel returns the enabled console log level.
func ConsoleLogLevel() string {
	return LogLevelName(*consoleLogger.LogLevel)
}

// ConsoleLogKeys returns the enabled console log keys.
func ConsoleLogKeys() []string {
	return consoleLogger.LogKey.EnabledLogKeys()
}

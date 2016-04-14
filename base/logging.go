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
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
	"github.com/couchbase/clog"
)

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

//Attach logger to stderr during load, this may get re-attached once config is loaded
func init() {
	logger = log.New(os.Stderr, "", 0)
	LogKeys = make(map[string]bool)
	logNoTime = false
}

func LogLevel() int {
	return logLevel
}

func SetLogLevel(level int) {
	logLock.Lock()
	defer logLock.Unlock()
	logLevel = level
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

// Parses a comma-separated list of log keys, probably coming from an argv flag.
// The key "bw" is interpreted as a call to LogNoColor, not a key.
func ParseLogFlag(flag string) {
	if flag != "" {
		ParseLogFlags(strings.Split(flag, ","))
	}
}

// Parses an array of log keys, probably coming from a argv flags.
// The key "bw" is interpreted as a call to LogNoColor, not a key.
func ParseLogFlags(flags []string) {
	logLock.Lock()
	for _, key := range flags {
		switch key {
		case "bw":
			LogNoColor()
		case "color":
			LogColor()
		case "notime":
			LogNoTime()
		default:
			LogKeys[key] = true
			if key == "*" {
				logStar = true
				EnableSgReplicateLogging()
			}
			// gocb requires a call into the gocb library to enable logging
			if key == "gocb" {
				EnableGoCBLogging()
			}
			if key == "Replicate" {
				EnableSgReplicateLogging()
			}
			for strings.HasSuffix(key, "+") {
				key = key[0 : len(key)-1]
				LogKeys[key] = true // "foo+" also enables "foo"
			}
		}
	}
	logLock.Unlock()
	Logf("Enabling logging: %s", flags)
}

func EnableSgReplicateLogging() {
	clog.EnableKey("Replicate")
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
	for k, v := range keys {
		LogKeys[k] = v
		if k == "*" {
			logStar = v
		}
	}
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

// Logs a message to the console, but only if the corresponding key is true in LogKeys.
func LogTo(key string, format string, args ...interface{}) {
	logLock.RLock()
	defer logLock.RUnlock()
	ok := logLevel <= 1 && (logStar || LogKeys[key])

	if ok {
		printf(fgYellow+key+": "+reset+format, args...)
	}
}

func EnableLogKey(key string) {
	logLock.Lock()
	defer logLock.Unlock()
	LogKeys[key] = true
}

func LogEnabled(key string) bool {
	logLock.RLock()
	defer logLock.RUnlock()
	return logStar || LogKeys[key]
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

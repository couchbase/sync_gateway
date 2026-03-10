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

	"github.com/couchbase/clog"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbaselabs/rosmar"
)

// remapGoCBLogLevels controls whether the gocb and gocbcore log levels are remapped to match the verbosity of SG's log levels.
const remapGoCBLogLevels = true

// This file implements wrappers around the loggers of external packages
// so that all of SG's logging output is consistent
func initExternalLoggers() {
	if remapGoCBLogLevels {
		gocb.SetLogger(GoCBLoggerRemapped{})
		gocbcore.SetLogger(GoCBCoreLoggerRemapped{})
	} else {
		gocb.SetLogger(GoCBLogger{})
		gocbcore.SetLogger(GoCBCoreLogger{})
	}

	clog.SetLoggerCallback(ClogCallback)
	// Set the clog level to DEBUG and do filtering for debug inside ClogCallback functions
	clog.SetLevel(clog.LevelDebug)

	// Redirect Walrus logging to SG logs, and set an appropriate level:
	rosmar.LoggingCallback = rosmarLogger

	updateExternalLoggers()
}

func updateExternalLoggers() {
	// use context.Background() since this is called from init or to reset test logging
	logger := consoleLogger.Load()
	if logger.shouldLog(context.Background(), LevelDebug, KeyWalrus) {
		rosmar.SetLogLevel(rosmar.LevelDebug)
	} else {
		rosmar.SetLogLevel(rosmar.LevelInfo)
	}
}

// **************************************************
// Implementation of github.com/couchbase/gocb.Logger
// **************************************************
type GoCBLogger struct{}

var _ gocb.Logger = GoCBLogger{}

// Log wraps the levelled SG logs for gocb to use. Log levels are not changed or remapped.
func (GoCBLogger) Log(level gocb.LogLevel, offset int, format string, v ...any) error {
	switch level {
	case gocb.LogError:
		logTo(context.TODO(), LevelError, KeyAll, KeyGoCB.String()+": "+format, v...)
	case gocb.LogWarn:
		logTo(context.TODO(), LevelWarn, KeyAll, KeyGoCB.String()+": "+format, v...)
	case gocb.LogInfo:
		logTo(context.TODO(), LevelInfo, KeyGoCB, format, v...)
	case gocb.LogDebug:
		logTo(context.TODO(), LevelDebug, KeyGoCB, format, v...)
	case gocb.LogTrace, gocb.LogMaxVerbosity:
		logTo(context.TODO(), LevelTrace, KeyGoCB, format, v...)
	case gocb.LogSched:
		logTo(context.TODO(), LevelTrace, KeyGoCB, "<SCHED>: "+format, v...)
	}
	return nil
}

type GoCBCoreLogger struct{}

var _ gocbcore.Logger = GoCBCoreLogger{}

func (GoCBCoreLogger) Log(level gocbcore.LogLevel, offset int, format string, v ...any) error {
	return GoCBLogger{}.Log(gocb.LogLevel(level), offset, format, v...)
}

// **************************************************
// Implementation of github.com/couchbase/gocb.Logger
// **************************************************
type GoCBLoggerRemapped struct{}

var _ gocb.Logger = GoCBLoggerRemapped{}

// Log wraps the levelled SG logs for gocb to use. Log levels are mapped as follows:
//
//	Error  -> SG Error
//	Warn   -> SG Warn
//	Info   -> SG Debug
//	Debug  -> SG Trace
//	Trace  -> SG Trace
//	Sched  -> SG Trace
//	Others -> no-op
func (GoCBLoggerRemapped) Log(level gocb.LogLevel, offset int, format string, v ...any) error {
	switch level {
	case gocb.LogError:
		logTo(context.TODO(), LevelError, KeyAll, KeyGoCB.String()+": "+format, v...)
	case gocb.LogWarn:
		logTo(context.TODO(), LevelWarn, KeyAll, KeyGoCB.String()+": "+format, v...)
	case gocb.LogInfo:
		logTo(context.TODO(), LevelDebug, KeyGoCB, format, v...)
	case gocb.LogDebug, gocb.LogTrace, gocb.LogSched, gocb.LogMaxVerbosity:
		logTo(context.TODO(), LevelTrace, KeyGoCB, format, v...)
	}
	return nil
}

type GoCBCoreLoggerRemapped struct{}

var _ gocbcore.Logger = GoCBCoreLoggerRemapped{}

func (GoCBCoreLoggerRemapped) Log(level gocbcore.LogLevel, offset int, format string, v ...any) error {
	return GoCBLoggerRemapped{}.Log(gocb.LogLevel(level), offset, format, v...)
}

// **************************************************************************
// Implementation of callback for github.com/couchbase/clog.SetLoggerCallback
//
//	Our main library that uses clog is cbgt, so all logging goes to KeyDCP.
//	Note that although sg-replicate uses clog's log levels, sgreplicateLogFn
//	bypasses clog logging, and so won't end up in this callback.
//
// **************************************************************************
func ClogCallback(level, format string, v ...any) string {
	switch level {
	case "ERRO", "FATA", "CRIT":
		logTo(context.TODO(), LevelError, KeyAll, KeyDCP.String()+": "+format, v...)
	case "WARN":
		// TODO: cbgt currently logs a lot of what we'd consider info as WARN,
		//    (i.e. diagnostic information that's not actionable by users), so
		//    routing to Info pending potential enhancements on cbgt side.
		logTo(context.TODO(), LevelInfo, KeyDCP, format, v...)
	case "INFO":
		// TODO: cbgt currently logs a lot of what we'd consider debug as INFO,
		//    (i.e. janitor work and partition status), so
		//    routing to Debug pending potential enhancements on cbgt side.
		logTo(context.TODO(), LevelDebug, KeyDCP, format, v...)
	case "DEBU":
		logTo(context.TODO(), LevelDebug, KeyDCP, format, v...)
	case "TRAC":
		logTo(context.TODO(), LevelTrace, KeyDCP, format, v...)
	}
	return ""
}

// **************************************************************************
// Log callback for Rosmar
// **************************************************************************
func rosmarLogger(level rosmar.LogLevel, fmt string, args ...any) {
	sgLevel := LogLevel(level)
	// info logging in Rosmar is extremely verbose (view queried, dcp message sent, etc.) - drop down to debug
	if level == rosmar.LevelInfo {
		sgLevel = LevelDebug
	}
	key := KeyWalrus
	if sgLevel <= LevelWarn {
		key = KeyAll
		fmt = "Rosmar: " + fmt
	}
	logTo(context.TODO(), sgLevel, key, fmt, args...)
}

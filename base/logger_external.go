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
	"os"

	"github.com/couchbase/clog"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/goutils/logging"
)

// This file implements wrappers around the loggers of external packages
// so that all of SG's logging output is consistent
func initExternalLoggers() {
	gocb.SetLogger(GoCBLogger{})
	gocbcore.SetLogger(GoCBCoreLogger{})

	logging.SetLogger(CBGoUtilsLogger{})
	clog.SetLoggerCallback(ClogCallback)
}

// **************************************************
// Implementation of github.com/couchbase/gocb.Logger
// **************************************************
type GoCBLogger struct{}

var _ gocb.Logger = GoCBLogger{}

// Log wraps the levelled SG logs for gocb to use.
// Log levels are mapped as follows:
//
//	Error  -> SG Error
//	Warn   -> SG Warn
//	Info   -> SG Debug
//	Debug  -> SG Trace
//	Trace  -> SG Trace
//	Others -> no-op
func (GoCBLogger) Log(level gocb.LogLevel, offset int, format string, v ...interface{}) error {
	switch level {
	case gocb.LogError:
		LogfTo(context.TODO(), LevelError, KeyAll, KeyGoCB.String()+": "+format, v...)
	case gocb.LogWarn:
		LogfTo(context.TODO(), LevelWarn, KeyAll, KeyGoCB.String()+": "+format, v...)
	case gocb.LogInfo:
		LogfTo(context.TODO(), LevelDebug, KeyGoCB, format, v...)
	case gocb.LogDebug, gocb.LogTrace:
		LogfTo(context.TODO(), LevelTrace, KeyGoCB, format, v...)
	}
	return nil
}

type GoCBCoreLogger struct{}
type GoCBCoreV7Logger struct{}

var _ gocbcore.Logger = GoCBCoreLogger{}

func (GoCBCoreLogger) Log(level gocbcore.LogLevel, offset int, format string, v ...interface{}) error {
	return GoCBLogger{}.Log(gocb.LogLevel(level), offset, format, v...)
}

// **************************************************************************
// Implementation of callback for github.com/couchbase/clog.SetLoggerCallback
//
//	Our main library that uses clog is cbgt, so all logging goes to KeyDCP.
//	Note that although sg-replicate uses clog's log levels, sgreplicateLogFn
//	bypasses clog logging, and so won't end up in this callback.
//
// **************************************************************************
func ClogCallback(level, format string, v ...interface{}) string {
	switch level {
	case "ERRO", "FATA", "CRIT":
		LogfTo(context.TODO(), LevelError, KeyAll, KeyDCP.String()+": "+format, v...)
	case "WARN":
		// TODO: cbgt currently logs a lot of what we'd consider info as WARN,
		//    (i.e. diagnostic information that's not actionable by users), so
		//    routing to Info pending potential enhancements on cbgt side.
		LogfTo(context.TODO(), LevelInfo, KeyDCP, format, v...)
	case "INFO":
		// TODO: cbgt currently logs a lot of what we'd consider debug as INFO,
		//    (i.e. janitor work and partition status), so
		//    routing to Debug pending potential enhancements on cbgt side.
		LogfTo(context.TODO(), LevelDebug, KeyDCP, format, v...)
	case "DEBU":
		LogfTo(context.TODO(), LevelDebug, KeyDCP, format, v...)
	case "TRAC":
		LogfTo(context.TODO(), LevelTrace, KeyDCP, format, v...)
	}
	return ""
}

// **************************************************************
// Implementation for github.com/couchbase/goutils/logging.Logger
// **************************************************************
type CBGoUtilsLogger struct{}

var _ logging.Logger = CBGoUtilsLogger{}

func (CBGoUtilsLogger) SetLevel(l logging.Level) {
	// no-op
}

// CBGoUtilsLogger.Level returns a compatible go-couchbase/golog Log Level for
// the given logging config LogLevel
func (CBGoUtilsLogger) Level() logging.Level {
	if consoleLogger == nil || consoleLogger.LogLevel == nil {
		return logging.INFO
	}
	switch *consoleLogger.LogLevel {
	case LevelTrace:
		return logging.TRACE
	case LevelDebug:
		return logging.DEBUG
	case LevelInfo:
		return logging.INFO
	case LevelWarn:
		return logging.WARN
	case LevelError:
		return logging.ERROR
	case LevelNone:
		return logging.NONE
	default:
		return logging.NONE
	}
}

func (CBGoUtilsLogger) Fatalf(fmt string, args ...interface{}) {
	LogfTo(context.TODO(), LevelError, KeyAll, "CBGoUtilsLogger: "+fmt, args...)
	FlushLogBuffers()
	os.Exit(1)
}

func (CBGoUtilsLogger) Severef(fmt string, args ...interface{}) {
	LogfTo(context.TODO(), LevelError, KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

func (CBGoUtilsLogger) Errorf(fmt string, args ...interface{}) {
	LogfTo(context.TODO(), LevelError, KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

func (CBGoUtilsLogger) Warnf(fmt string, args ...interface{}) {
	LogfTo(context.TODO(), LevelWarn, KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

func (CBGoUtilsLogger) Infof(fmt string, args ...interface{}) {
	LogfTo(context.TODO(), LevelInfo, KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

func (CBGoUtilsLogger) Requestf(rlevel logging.Level, fmt string, args ...interface{}) {
	LogfTo(context.TODO(), LevelTrace, KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

func (CBGoUtilsLogger) Tracef(fmt string, args ...interface{}) {
	LogfTo(context.TODO(), LevelTrace, KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

func (CBGoUtilsLogger) Debugf(fmt string, args ...interface{}) {
	LogfTo(context.TODO(), LevelDebug, KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

func (CBGoUtilsLogger) Logf(level logging.Level, fmt string, args ...interface{}) {
	LogfTo(context.TODO(), LevelInfo, KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

// go-couchbase/gomemcached don't use Pair/Map logs, so these are all stubs

func (l CBGoUtilsLogger) Loga(level logging.Level, f func() string) { l.warnNotImplemented("Loga", f) }
func (l CBGoUtilsLogger) Debuga(f func() string)                    { l.warnNotImplemented("Debuga", f) }
func (l CBGoUtilsLogger) Tracea(f func() string)                    { l.warnNotImplemented("Tracea", f) }
func (l CBGoUtilsLogger) Requesta(rlevel logging.Level, f func() string) {
	l.warnNotImplemented("Requesta", f)
}
func (l CBGoUtilsLogger) Infoa(f func() string)   { l.warnNotImplemented("Infoa", f) }
func (l CBGoUtilsLogger) Warna(f func() string)   { l.warnNotImplemented("Warna", f) }
func (l CBGoUtilsLogger) Errora(f func() string)  { l.warnNotImplemented("Errora", f) }
func (l CBGoUtilsLogger) Severea(f func() string) { l.warnNotImplemented("Severea", f) }
func (l CBGoUtilsLogger) Fatala(f func() string)  { l.warnNotImplemented("Fatala", f) }

func (CBGoUtilsLogger) warnNotImplemented(name string, f func() string) {
	WarnfCtx(context.Background(), fmt.Sprintf("CBGoUtilsLogger: %s not implemented! %s", name, f()))
}

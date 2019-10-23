package base

import (
	"github.com/couchbase/clog"
	"github.com/couchbase/gocb"
	"github.com/couchbase/goutils/logging"
	"gopkg.in/couchbase/gocbcore.v7"
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
//   Error  -> SG Error
//   Warn   -> SG Warn
//   Info   -> SG Debug
//   Debug  -> SG Trace
//   Trace  -> SG Trace
//   Others -> no-op
func (GoCBLogger) Log(level gocb.LogLevel, offset int, format string, v ...interface{}) error {
	switch level {
	case gocb.LogError:
		Errorf(KeyGoCB, format, v...)
	case gocb.LogWarn:
		Warnf(KeyGoCB, format, v...)
	case gocb.LogInfo:
		Debugf(KeyGoCB, format, v...)
	case gocb.LogDebug, gocb.LogTrace:
		Tracef(KeyGoCB, format, v...)
	}
	return nil
}

// ******************************************************
// Implementation of github.com/couchbase/gocbcore.Logger
// ******************************************************
type GoCBCoreLogger struct{}

var _ gocbcore.Logger = GoCBCoreLogger{}

// Log wraps the levelled SG logs for gocbcore to use.
// Log levels are mapped as follows:
//   Error  -> SG Error
//   Warn   -> SG Warn
//   Info   -> SG Debug
//   Debug  -> SG Trace
//   Trace  -> SG Trace
//   Others -> no-op
func (GoCBCoreLogger) Log(level gocbcore.LogLevel, offset int, format string, v ...interface{}) error {
	switch level {
	case gocbcore.LogError:
		Errorf(KeyGoCB, format, v...)
	case gocbcore.LogWarn:
		Warnf(KeyGoCB, format, v...)
	case gocbcore.LogInfo:
		Debugf(KeyGoCB, format, v...)
	case gocbcore.LogDebug, gocbcore.LogTrace:
		Tracef(KeyGoCB, format, v...)
	}
	return nil
}

// ******************************************************
// SG Replicate Logging
// ******************************************************
// Log levels are mapped as follows:
//   Debug   -> SG Debug
//   Normal  -> SG Info
//   Warning -> SG Warn
//   Error   -> SG Error
//   Panic   -> SG Error

func sgreplicateLogFn(level clog.LogLevel, format string, args ...interface{}) {
	switch level {
	case clog.LevelDebug:
		Debugf(KeyReplicate, format, args...)
	case clog.LevelNormal:
		Infof(KeyReplicate, format, args...)
	case clog.LevelWarning:
		Warnf(KeyReplicate, format, args...)
	case clog.LevelError:
		Errorf(KeyReplicate, format, args...)
	case clog.LevelPanic:
		Errorf(KeyReplicate, format, args...)
	}
}

// **************************************************************************
// Implementation of callback for github.com/couchbase/clog.SetLoggerCallback
//    Our main library that uses clog is cbgt, so all logging goes to KeyDCP.
//    Note that although sg-replicate uses clog's log levels, sgreplicateLogFn
//    bypasses clog logging, and so won't end up in this callback.
// **************************************************************************
func ClogCallback(level, format string, v ...interface{}) string {
	switch level {
	case "ERRO", "FATA", "CRIT":
		Errorf(KeyDCP, format, v...)
	case "WARN":
		// TODO: cbgt currently logs a lot of what we'd consider info as WARN,
		//    (i.e. diagnostic information that's not actionable by users), so
		//    routing to Info pending potential enhancements on cbgt side.
		Infof(KeyDCP, format, v...)
	case "INFO":
		// TODO: cbgt currently logs a lot of what we'd consider debug as INFO,
		//    (i.e. diagnostic information that's not actionable by users), so
		//    routing to Info pending potential enhancements on cbgt side.
		Debugf(KeyDCP, format, v...)
	case "DEBU":
		Tracef(KeyDCP, format, v...)
	case "TRAC":
		Tracef(KeyDCP, format, v...)
	}
	return ""
}

// **************************************************************
// Implementation for github.com/couchbase/goutils/logging.Logger
// **************************************************************
type CBGoUtilsLogger struct{}

var _ logging.Logger = CBGoUtilsLogger{}

func (CBGoUtilsLogger) SetLevel(l logging.Level) {
	return // no-op
}

func (CBGoUtilsLogger) Level() logging.Level {
	if consoleLogger == nil || consoleLogger.LogLevel == nil {
		return logging.INFO
	}
	return ToDeprecatedLogLevel(*consoleLogger.LogLevel).cgLevel()
}

func (CBGoUtilsLogger) Fatalf(fmt string, args ...interface{}) {
	Fatalf(KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

func (CBGoUtilsLogger) Severef(fmt string, args ...interface{}) {
	Errorf(KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

func (CBGoUtilsLogger) Errorf(fmt string, args ...interface{}) {
	Errorf(KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

func (CBGoUtilsLogger) Warnf(fmt string, args ...interface{}) {
	Warnf(KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

func (CBGoUtilsLogger) Infof(fmt string, args ...interface{}) {
	Infof(KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

func (CBGoUtilsLogger) Requestf(rlevel logging.Level, fmt string, args ...interface{}) {
	Tracef(KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

func (CBGoUtilsLogger) Tracef(fmt string, args ...interface{}) {
	Tracef(KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

func (CBGoUtilsLogger) Debugf(fmt string, args ...interface{}) {
	Debugf(KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

func (CBGoUtilsLogger) Logf(level logging.Level, fmt string, args ...interface{}) {
	Infof(KeyAll, "CBGoUtilsLogger: "+fmt, args...)
}

// go-couchbase/gomemcached don't use Pair/Map logs, so these are all stubs
func (CBGoUtilsLogger) Fatalm(msg string, kv logging.Map) {
	Warnf(KeyAll, "CBGoUtilsLogger: Fatalm not implemented! "+msg)
}

func (CBGoUtilsLogger) Fatalp(msg string, kv ...logging.Pair) {
	Warnf(KeyAll, "CBGoUtilsLogger: Fatalp not implemented! "+msg)
}

func (CBGoUtilsLogger) Severem(msg string, kv logging.Map) {
	Warnf(KeyAll, "CBGoUtilsLogger: Severem not implemented! "+msg)
}

func (CBGoUtilsLogger) Severep(msg string, kv ...logging.Pair) {
	Warnf(KeyAll, "CBGoUtilsLogger: Severep not implemented! "+msg)
}

func (CBGoUtilsLogger) Errorm(msg string, kv logging.Map) {
	Warnf(KeyAll, "CBGoUtilsLogger: Errorm not implemented! "+msg)
}

func (CBGoUtilsLogger) Errorp(msg string, kv ...logging.Pair) {
	Warnf(KeyAll, "CBGoUtilsLogger: Errorp not implemented! "+msg)
}

func (CBGoUtilsLogger) Warnm(msg string, kv logging.Map) {
	Warnf(KeyAll, "CBGoUtilsLogger: Warnm not implemented! "+msg)
}

func (CBGoUtilsLogger) Warnp(msg string, kv ...logging.Pair) {
	Warnf(KeyAll, "CBGoUtilsLogger: Warnp not implemented! "+msg)
}

func (CBGoUtilsLogger) Infom(msg string, kv logging.Map) {
	Warnf(KeyAll, "CBGoUtilsLogger: Infom not implemented! "+msg)
}

func (CBGoUtilsLogger) Infop(msg string, kv ...logging.Pair) {
	Warnf(KeyAll, "CBGoUtilsLogger: Infop not implemented! "+msg)
}

func (CBGoUtilsLogger) Requestm(rlevel logging.Level, msg string, kv logging.Map) {
	Warnf(KeyAll, "CBGoUtilsLogger: Requestm not implemented! "+msg)
}

func (CBGoUtilsLogger) Requestp(rlevel logging.Level, msg string, kv ...logging.Pair) {
	Warnf(KeyAll, "CBGoUtilsLogger: Requestp not implemented! "+msg)
}

func (CBGoUtilsLogger) Tracem(msg string, kv logging.Map) {
	Warnf(KeyAll, "CBGoUtilsLogger: Tracem not implemented! "+msg)
}

func (CBGoUtilsLogger) Tracep(msg string, kv ...logging.Pair) {
	Warnf(KeyAll, "CBGoUtilsLogger: Tracep not implemented! "+msg)
}

func (CBGoUtilsLogger) Debugm(msg string, kv logging.Map) {
	Warnf(KeyAll, "CBGoUtilsLogger: Debugm not implemented! "+msg)
}

func (CBGoUtilsLogger) Debugp(msg string, kv ...logging.Pair) {
	Warnf(KeyAll, "CBGoUtilsLogger: Debugp not implemented! "+msg)
}

func (CBGoUtilsLogger) Logm(level logging.Level, msg string, kv logging.Map) {
	Warnf(KeyAll, "CBGoUtilsLogger: Logm not implemented! "+msg)
}

func (CBGoUtilsLogger) Logp(level logging.Level, msg string, kv ...logging.Pair) {
	Warnf(KeyAll, "CBGoUtilsLogger: Logp not implemented! "+msg)
}

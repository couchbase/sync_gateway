// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	ansiReset  = "\x1b[0m"
	ansiGray   = "\x1b[90m"
	ansiYellow = "\x1b[33m"
	ansiRed    = "\x1b[31m"
	ansiCyan   = "\x1b[36m"
)

// shortLevelEncoder encodes log levels as fixed 5-character uppercase abbreviations.
func shortLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	switch l {
	case zapcore.DebugLevel:
		enc.AppendString("DEBUG")
	case zapcore.InfoLevel:
		enc.AppendString("INFO ")
	case zapcore.WarnLevel:
		enc.AppendString("WRN  ")
	case zapcore.ErrorLevel:
		enc.AppendString("ERROR")
	default:
		enc.AppendString(fmt.Sprintf("%-5s", strings.ToUpper(l.String())))
	}
}

// shortColorLevelEncoder is shortLevelEncoder with ANSI colour codes for terminal output.
func shortColorLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	switch l {
	case zapcore.DebugLevel:
		enc.AppendString(ansiGray + "DEBUG" + ansiReset)
	case zapcore.InfoLevel:
		enc.AppendString(ansiCyan + "INFO " + ansiReset)
	case zapcore.WarnLevel:
		enc.AppendString(ansiYellow + "WRN  " + ansiReset)
	case zapcore.ErrorLevel:
		enc.AppendString(ansiRed + "ERROR" + ansiReset)
	default:
		enc.AppendString(fmt.Sprintf("%-5s", strings.ToUpper(l.String())))
	}
}

func initLogger(verbose bool) {
	level := zapcore.InfoLevel
	if verbose {
		level = zapcore.DebugLevel
	}
	encCfg := zap.NewDevelopmentEncoderConfig()
	encCfg.EncodeTime = nil
	// Use colour encoding only when stderr is a real terminal; piped output
	// would otherwise contain raw ANSI escape sequences.
	if isTerminal(os.Stderr) {
		encCfg.EncodeLevel = shortColorLevelEncoder
	} else {
		encCfg.EncodeLevel = shortLevelEncoder
	}
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encCfg),
		zapcore.AddSync(os.Stderr),
		level,
	)
	logger = zap.New(core, zap.WithCaller(false)).Sugar()
}

// isTerminal reports whether f is connected to a real terminal (not a pipe or file).
func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// printCommand logs a command in shell-trace style (like set -x), followed by
// any active extraEnv overrides and per-invocation env extras.
func printCommand(cmd *exec.Cmd, envExtras ...string) {
	logger.Debugf("+ %s", strings.Join(cmd.Args, " "))
	for k, v := range extraEnv {
		logger.Debugf("  env %s=%s", k, v)
	}
	for _, e := range envExtras {
		logger.Debugf("  env %s", e)
	}
}

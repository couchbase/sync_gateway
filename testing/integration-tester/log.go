// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"bytes"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func initLogger(verbose bool) {
	level := zapcore.InfoLevel
	if verbose {
		level = zapcore.DebugLevel
	}
	encCfg := zap.NewDevelopmentEncoderConfig()
	encCfg.EncodeTime = nil
	encCfg.EncodeLevel = nil
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encCfg),
		zapcore.AddSync(os.Stderr),
		level,
	)
	logger = zap.New(core, zap.WithCaller(false)).Sugar()
}

// printCommand logs a command in shell-trace style (like set -x), followed by
// any active extraEnv overrides and per-invocation env extras.
func printCommand(prefix string, cmd *exec.Cmd, envExtras ...string) {
	logger.Debugf("%s+ %s", prefix, strings.Join(cmd.Args, " "))
	for k, v := range extraEnv {
		logger.Debugf("%s  env %s=%s", prefix, k, v)
	}
	for _, e := range envExtras {
		logger.Debugf("%s  env %s", prefix, e)
	}
}

// labelPrefix returns the "[label] " prefix for log lines that belong to one package, or "" for no label.
func labelPrefix(label string) string {
	if label == "" {
		return ""
	}
	return "[" + label + "] "
}

// labelWriter logs each complete line written to it at info level with a prefix.
// Call flush after the writer's last write to log any trailing partial line.
type labelWriter struct {
	prefix string
	buf    []byte
}

func (w *labelWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for i := bytes.IndexByte(w.buf, '\n'); i >= 0; i = bytes.IndexByte(w.buf, '\n') {
		logger.Info(w.prefix + strings.TrimRight(string(w.buf[:i]), "\r"))
		w.buf = w.buf[i+1:]
	}
	return len(p), nil
}

func (w *labelWriter) flush() {
	if len(w.buf) > 0 {
		logger.Info(w.prefix + string(w.buf))
		w.buf = nil
	}
}

// sgTimestampPattern matches the date and zone of an ISO 8601 Sync Gateway log timestamp, keeping the time of day.
var sgTimestampPattern = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T(\d{2}:\d{2}:\d{2}\.\d{3})(?:Z|[+-]\d{2}:\d{2})`)

// shortenTimestamps rewrites Sync Gateway log timestamps to HH:MM:SS.mmm for console output.
func shortenTimestamps(line string) string {
	return sgTimestampPattern.ReplaceAllString(line, "$1")
}

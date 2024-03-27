//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package mqtt

import (
	"context"
	"io"
	"log"
	"log/slog"
	"strings"

	"github.com/couchbase/sync_gateway/base"
)

//======== slog handler

// Implementation of `slog.Handler` that routes `slog` messages to SG logging.
type slogHandler struct {
	key          base.LogKey
	name         string
	baseAttrsStr string
}

// Creates a new `slog.Logger` that writes to SG logging. Messages are prefixed with `name`.
func newSlogger(key base.LogKey, name string) *slog.Logger {
	return slog.New(&slogHandler{key: key, name: name})
}

func slogToBaseLevel(level slog.Level) base.LogLevel {
	if level < slog.LevelDebug {
		return base.LevelTrace
	} else if level < slog.LevelInfo {
		return base.LevelDebug
	} else if level < slog.LevelWarn {
		return base.LevelInfo
	} else if level < slog.LevelError {
		return base.LevelWarn
	} else {
		return base.LevelError
	}
}

func (slh *slogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	switch slogToBaseLevel(level) {
	case base.LevelTrace:
		return base.LogTraceEnabled(ctx, base.KeyMQTT)
	case base.LevelDebug:
		return base.LogDebugEnabled(ctx, base.KeyMQTT)
	case base.LevelInfo:
		return base.LogInfoEnabled(ctx, base.KeyMQTT)
	default:
		return true
	}
}

func (slh *slogHandler) Handle(ctx context.Context, record slog.Record) error {
	level := slogToBaseLevel(record.Level)
	attrsStr := slh.baseAttrsStr
	record.Attrs(func(a slog.Attr) bool {
		if !a.Equal(slog.Attr{}) {
			if attrsStr != "" {
				attrsStr += ", "
			}
			attrsStr += a.String()
		}
		return true
	})

	base.LogLevelCtx(ctx, level, slh.key, "(%s) %s: %s", slh.name, record.Message, attrsStr)
	return nil
}

func (slh *slogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	attrsStr := slh.baseAttrsStr
	for _, attr := range attrs {
		if attrsStr != "" {
			attrsStr += ", "
		}
		attrsStr += attr.String()
	}
	return &slogHandler{baseAttrsStr: attrsStr}
}

func (slh *slogHandler) WithGroup(name string) slog.Handler {
	// Mochi MQTT does not appear to call WithGroup, so I'm ignoring it.
	base.InfofCtx(context.Background(), base.KeyMQTT, "WithGroup: %q -- ignoring", name)
	return slh
}

//======== log.Logger adapter

// Creates a `log.Logger` that writes to SG logs.
func newLogLogger(ctx context.Context, key base.LogKey, prefix string) *log.Logger {
	return log.New(&logWriter{ctx: ctx, key: key}, prefix, 0)
}

// Implementation of io.Writer that accepts log messages and sends them to SG logs.
type logWriter struct {
	ctx context.Context
	key base.LogKey
}

func (lw *logWriter) Write(p []byte) (n int, err error) {
	// (This is sort of a kludge; but the log.Logger doesn't give any metadata,
	// just the line of text to log.)
	message := string(p)
	var level base.LogLevel = base.LevelInfo
	if strings.HasPrefix(message, "[DEBUG] ") {
		level = base.LevelDebug
		message = message[8:]
	} else if strings.HasPrefix(message, "[INFO] ") {
		level = base.LevelInfo
		message = message[7:]
	} else if strings.HasPrefix(message, "[WARN] ") {
		level = base.LevelWarn
		message = message[7:]
	} else if strings.HasPrefix(message, "[ERROR] ") {
		level = base.LevelError
		message = message[8:]
	}
	base.LogLevelCtx(lw.ctx, level, lw.key, "%s", message)
	return len(p), nil
}

var (
	// Enforce interface conformance:
	_ slog.Handler = &slogHandler{}
	_ io.Writer    = &logWriter{}
)

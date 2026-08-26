// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"bufio"
	"errors"
	"net"
	"net/http"
)

// NonCountedResponseWriter is a passhtrough http.ResponseWriter that satisfies the CoutableResponseWriter interface.
type NonCountedResponseWriter struct {
	http.ResponseWriter
	// beforeWriteHeader, if set, runs immediately before headers are flushed - the last point at
	// which a header can still be added.
	beforeWriteHeader func()
	headerWritten     bool
}

// WriteHeader runs beforeWriteHeader once, then flushes.
func (w *NonCountedResponseWriter) WriteHeader(status int) {
	if !w.headerWritten {
		w.headerWritten = true
		if w.beforeWriteHeader != nil {
			w.beforeWriteHeader()
		}
	}
	w.ResponseWriter.WriteHeader(status)
}

// NewNonCountedResponseWriter returns a new NonCountedResponseWriter that wraps the given ResponseWriter.
func NewNonCountedResponseWriter(writer http.ResponseWriter) *NonCountedResponseWriter {
	return &NonCountedResponseWriter{
		ResponseWriter: writer,
	}
}

// reportStats is a no-op to satisfy the CountableResponseWriter interface
func (w *NonCountedResponseWriter) reportStats(updateImmediately bool) {
}

// Hijack implement http.Hijcker interface to satisfy the upgrade to websockets
func (w *NonCountedResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("underlying ResponseWriter doesn't support http.Hijacker interface")
	}
	return h.Hijack()
}

// isHijackable determines if the underlying writer implements hijack interface
func (w *NonCountedResponseWriter) isHijackable() bool {
	_, ok := w.ResponseWriter.(http.Hijacker)
	return ok
}

func (w *NonCountedResponseWriter) Flush() {
	w.ResponseWriter.(http.Flusher).Flush()
}

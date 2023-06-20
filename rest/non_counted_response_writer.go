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

// NonCountedResponseWriter is a passhtrough http.ResponseWriter that satisifes the CoutableResponseWriter interface.
type NonCountedResponseWriter struct {
	writer http.ResponseWriter
}

// NewNonCountedResponseWriter returns a new NonCountedResponseWriter that wraps the given ResponseWriter.
func NewNonCountedResponseWriter(writer http.ResponseWriter) *NonCountedResponseWriter {
	return &NonCountedResponseWriter{
		writer: writer,
	}
}

// Header passes through to the underlying ResponseWriter
func (w *NonCountedResponseWriter) Header() http.Header {
	return w.writer.Header()
}

// Write passes through to the underlying ResponseWriter while incrementing the number of bytes.
func (w *NonCountedResponseWriter) Write(b []byte) (int, error) {
	return w.writer.Write(b)
}

// WriteHeader passes through to the underlying ResponseWriter
func (w *NonCountedResponseWriter) WriteHeader(statusCode int) {
	w.writer.WriteHeader(statusCode)
}

// ReportStats is a no-op to satisfy the CountableResponseWriter interface
func (w *NonCountedResponseWriter) reportStats(updateImmediately bool) {
}

// Hijack implement http.Hijcker interface to satisfy the upgrade to websockets
func (w *NonCountedResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.writer.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("underlying ResponseWriter doesn't support http.Hijacker interface")
	}
	return h.Hijack()
}

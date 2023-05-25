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

// CountableResponseWriter is an interface that any custom http.ResponseWriter used by Sync Gateway handlers need to implement.
type CountableResponseWriter interface {
	http.ResponseWriter
	GetBytesWritten() int64
}

// http.ResponseWriter wrapper that counts the number of bytes written in a response. This ignores the bytes written in headers.
type CountedResponseWriter struct {
	writer       http.ResponseWriter
	bytesWritten int64
}

var _ CountedResponseWriter = CountedResponseWriter{}

// Header passes through to the underlying ResponseWriter
func (w *CountedResponseWriter) Header() http.Header {
	return w.writer.Header()
}

// Write passes through to the underlying ResponseWriter while incrementing the number of bytes.
func (w *CountedResponseWriter) Write(b []byte) (int, error) {
	n, err := w.writer.Write(b)
	w.bytesWritten += int64(n)
	return n, err
}

// WriteHeader passes through to the underlying ResponseWriter
func (w *CountedResponseWriter) WriteHeader(statusCode int) {
	w.writer.WriteHeader(statusCode)
}

// GetBytesWritten returns the number of bytes written by this response writer. This is not locked, so is only safe to call while no one is calling CountedResponseWriter.Write, usually after the response has been fully written.
func (w *CountedResponseWriter) GetBytesWritten() int64 {
	return w.bytesWritten
}

// Hijack implement http.Hijcker interface to satisfy the upgrade to websockets
func (w *CountedResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.writer.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("underlying ResponseWriter doesn't support http.Hijacker interface")
	}
	return h.Hijack()
}

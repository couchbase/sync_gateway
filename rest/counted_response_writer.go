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

type CountableResponseWriter interface {
	http.ResponseWriter
	GetBytesWritten() int64
}

type CountedResponseWriter struct {
	writer       http.ResponseWriter
	bytesWritten int64
}

var _ CountedResponseWriter = CountedResponseWriter{}

func (w *CountedResponseWriter) Header() http.Header {
	return w.writer.Header()
}

func (w *CountedResponseWriter) Write(b []byte) (int, error) {
	n, err := w.writer.Write(b)
	w.bytesWritten += int64(n)
	return n, err
}

func (w *CountedResponseWriter) WriteHeader(statusCode int) {
	w.writer.WriteHeader(statusCode)
}

func (w *CountedResponseWriter) GetBytesWritten() int64 {
	return w.bytesWritten
}

func (w *CountedResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.writer.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("underlying ResponseWriter doesn't support http.Hijacker interface")
	}
	return h.Hijack()
}

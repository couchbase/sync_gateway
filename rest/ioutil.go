/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"io"

	"github.com/couchbase/sync_gateway/base"

	"net/http"
	"net/url"
)

// A TeeReader wrapper that can wrap an io.ReadCloser as opposed to just
// wrapping an io.Reader
type TeeReadCloser struct {
	r io.Reader
	w io.Writer
	c io.Closer
}

func NewTeeReadCloser(r io.ReadCloser, w io.Writer) io.ReadCloser {
	return &TeeReadCloser{
		r: io.TeeReader(r, w),
		w: w,
		c: r,
	}
}

func (t *TeeReadCloser) Read(b []byte) (int, error) {
	return t.r.Read(b)
}

func (t *TeeReadCloser) Close() error {
	return t.c.Close()
}

// A ResponseWriter that wraps another ResponseWriter, and dumps a copy of everything to
// the logging key
type LoggingTeeResponseWriter struct {
	http.ResponseWriter
	LogKey       base.LogKey   // The log key to use, eg base.KeyHTTP
	SerialNumber string        // The request ID
	Request      *http.Request // The request
	QueryValues  url.Values    // A cached copy of the URL query values
}

func NewLoggerTeeResponseWriter(wrappedResponseWriter http.ResponseWriter, logKey base.LogKey, serialNum string, req *http.Request, queryValues url.Values) http.ResponseWriter {
	return &LoggingTeeResponseWriter{
		ResponseWriter: wrappedResponseWriter,
		LogKey:         logKey,
		SerialNumber:   serialNum,
		Request:        req,
		QueryValues:    queryValues,
	}
}

func (l *LoggingTeeResponseWriter) Write(b []byte) (int, error) {
	base.Infof(l.LogKey, " %s: %s %s %s", l.SerialNumber, l.Request.Method, base.SanitizeRequestURL(l.Request, &l.QueryValues), base.UD(string(b)))
	return l.ResponseWriter.Write(b)
}
